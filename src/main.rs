mod config;
mod dlq;
mod flush;
mod models;
mod ttl_index;
mod tuning;

use anyhow::Result;
use bson::Document;
use config::Config;
use flush::flush_logs;
use models::{EventLog, GuardianLog};
use mongodb::{Client, Collection, options::ClientOptions};
use redis::aio::MultiplexedConnection;
use tokio::time::Duration;
use tracing::{error, info};

use crate::{dlq::dlq_reprocessor, models::JoinLog, tuning::adjust_tuning};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = Config::from_env();

    // Redis
    let redis_client = redis::Client::open(cfg.redis_url.clone())?;
    let redis_conn: MultiplexedConnection = redis_client.get_multiplexed_async_connection().await?;

    // Mongo
    let mongo_client = Client::with_options(ClientOptions::parse(&cfg.mongo_url).await?)?;
    let db = mongo_client.database(&cfg.mongo_db);

    let events_collection: Collection<Document> = db.collection("events");
    let guardian_collection: Collection<Document> = db.collection("guardianLogs");
    let join_collection: Collection<Document> = db.collection("joinLogs");

    // ttl indexing
    ttl_index::ensure_ttl_indexes(&events_collection, &guardian_collection, &join_collection)
        .await?;

    info!(
        "🚀 Log worker started with batch={} interval={}s",
        cfg.batch_size, cfg.flush_interval
    );

    let dlq_conn = redis_conn.clone();
    let dlq_collection = events_collection.clone(); // or a dedicated "failed" collection
    tokio::spawn(async move {
        if let Err(e) = dlq_reprocessor(dlq_conn, dlq_collection, cfg.batch_size).await {
            error!("❌ DLQ reprocessor stopped: {:?}", e);
        }
    });

    tokio::select! {
        _ = worker_loop(redis_conn.clone(), events_collection.clone(), guardian_collection.clone(),join_collection.clone(), cfg.clone()) => {},
        _ = shutdown_signal(redis_conn.clone(), events_collection.clone(), guardian_collection.clone(),join_collection.clone(), cfg.clone()) => {},
    }

    Ok(())
}

async fn worker_loop(
    redis_conn: MultiplexedConnection,
    events_collection: mongodb::Collection<EventLog>,
    guardian_collection: mongodb::Collection<GuardianLog>,
    join_collection: mongodb::Collection<JoinLog>,
    cfg: Config,
) {
    loop {
        let mut conn1 = redis_conn.clone();
        let mut conn2 = redis_conn.clone();
        let mut conn3 = redis_conn.clone();
        let mut conn4 = redis_conn.clone();

        let mut cmd_events = redis::cmd("LLEN");
        cmd_events.arg("logs:events");

        let mut cmd_guardian = redis::cmd("LLEN");
        cmd_guardian.arg("logs:guardian");

        let mut cmd_join = redis::cmd("LLEN");
        cmd_join.arg("logs:join");

        let (events_len, guardian_len, join_len) = tokio::join!(
            cmd_events.query_async::<usize>(&mut conn3),
            cmd_guardian.query_async::<usize>(&mut conn4),
            cmd_join.query_async::<usize>(&mut conn2)
        );

        let total_queue =
            events_len.unwrap_or(0) + guardian_len.unwrap_or(0) + join_len.unwrap_or(0);

        let (batch_size, interval) = adjust_tuning(total_queue, cfg.batch_size, cfg.flush_interval);

        let (events_res, guardian_res, join_res) = tokio::join!(
            flush_logs(&mut conn1, &events_collection, "logs:events", batch_size),
            flush_logs(
                &mut conn2,
                &guardian_collection,
                "logs:guardian",
                batch_size
            ),
            flush_logs(&mut conn3, &join_collection, "logs:join", batch_size)
        );

        match events_res {
            Ok(count) if count > 0 => {
                tracing::info!(count, batch_size, interval, "✅ Flushed event logs")
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error=?e, "❌ Error flushing events"),
        }

        match guardian_res {
            Ok(count) if count > 0 => {
                tracing::info!(count, batch_size, interval, "✅ Flushed guardian logs")
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error=?e, "❌ Error flushing guardian logs"),
        }

        match join_res {
            Ok(count) if count > 0 => {
                tracing::info!(count, batch_size, interval, "✅ Flushed join logs")
            }
            Ok(_) => {}
            Err(e) => tracing::error!(error=?e, "❌ Error flushing join logs"),
        }

        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

async fn shutdown_signal(
    mut redis_conn: MultiplexedConnection,
    events_collection: mongodb::Collection<EventLog>,
    guardian_collection: mongodb::Collection<GuardianLog>,
    join_collection: mongodb::Collection<JoinLog>,
    cfg: Config,
) {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for shutdown signal");
    info!("🛑 Shutdown signal received, flushing remaining logs...");

    if let Ok(count) = flush_logs(
        &mut redis_conn,
        &events_collection,
        "logs:events",
        cfg.batch_size,
    )
    .await
    {
        if count > 0 {
            info!("Flushed {} event logs on shutdown", count);
        }
    }
    if let Ok(count) = flush_logs(
        &mut redis_conn,
        &guardian_collection,
        "logs:guardian",
        cfg.batch_size,
    )
    .await
    {
        if count > 0 {
            info!("Flushed {} guardian logs on shutdown", count);
        }
    }

    if let Ok(count) = flush_logs(
        &mut redis_conn,
        &join_collection,
        "logs:join",
        cfg.batch_size,
    )
    .await
    {
        if count > 0 {
            info!("Flushed {} join logs on shutdown", count);
        }
    }

    info!("✅ Shutdown complete, exiting cleanly");
}
