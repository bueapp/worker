mod config;
mod flush;
mod models;
mod ttl_index;

use anyhow::Result;
use bson::Document;
use config::Config;
use flush::flush_logs;
use models::{EventLog, GuardianLog};
use mongodb::{Client, Collection, options::ClientOptions};
use redis::aio::MultiplexedConnection;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

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

    // ttl indexing
    ttl_index::ensure_ttl_indexes(&events_collection, &guardian_collection).await?;

    info!(
        "🚀 Log worker started with batch={} interval={}s",
        cfg.batch_size, cfg.flush_interval
    );

    tokio::select! {
        _ = worker_loop(redis_conn.clone(), events_collection.clone(), guardian_collection.clone(), cfg.clone()) => {},
        _ = shutdown_signal(redis_conn.clone(), events_collection.clone(), guardian_collection.clone(), cfg.clone()) => {},
    }

    Ok(())
}

async fn worker_loop(
    redis_conn: MultiplexedConnection,
    events_collection: mongodb::Collection<EventLog>,
    guardian_collection: mongodb::Collection<GuardianLog>,
    cfg: Config,
) {
    loop {
        let mut conn1 = redis_conn.clone();
        let mut conn2 = redis_conn.clone();

        let (events_res, guardian_res) = tokio::join!(
            flush_logs(
                &mut conn1,
                &events_collection,
                "logs:events",
                cfg.batch_size
            ),
            flush_logs(
                &mut conn2,
                &guardian_collection,
                "logs:guardian",
                cfg.batch_size
            )
        );

        if let Ok(count) = events_res {
            if count > 0 {
                info!("✅ Flushed {} event logs", count);
            }
        } else if let Err(e) = events_res {
            error!("❌ Error flushing events: {:?}", e);
        }

        if let Ok(count) = guardian_res {
            if count > 0 {
                info!("✅ Flushed {} guardian logs", count);
            }
        } else if let Err(e) = guardian_res {
            error!("❌ Error flushing guardian logs: {:?}", e);
        }

        sleep(Duration::from_secs(cfg.flush_interval)).await;
    }
}

async fn shutdown_signal(
    mut redis_conn: MultiplexedConnection,
    events_collection: mongodb::Collection<EventLog>,
    guardian_collection: mongodb::Collection<GuardianLog>,
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

    info!("✅ Shutdown complete, exiting cleanly");
}
