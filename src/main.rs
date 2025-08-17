mod config;
mod flush;
mod metrics;
mod models;

use anyhow::Result;
use chrono::Utc;
use config::Config;
use flush::flush_logs;
use metrics::{Metrics, run_metrics_server};
use models::{EventLog, GuardianLog};
use mongodb::{Client, options::ClientOptions};
use redis::aio::MultiplexedConnection;
use std::sync::{Arc, Mutex};
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

    let events_collection = db.collection::<EventLog>("events");
    let guardian_collection = db.collection::<GuardianLog>("guardianLogs");

    // Metrics
    let metrics = Arc::new(Mutex::new(Metrics::default()));

    // Spawn metrics server
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        run_metrics_server(metrics_clone, cfg.metrics_port).await;
    });

    info!(
        "🚀 Log worker started with batch={} interval={}s",
        cfg.batch_size, cfg.flush_interval
    );

    tokio::select! {
        _ = worker_loop(redis_conn.clone(), events_collection.clone(), guardian_collection.clone(), metrics.clone(), cfg.clone()) => {},
        _ = shutdown_signal(redis_conn.clone(), events_collection.clone(), guardian_collection.clone(), cfg.clone()) => {},
    }

    Ok(())
}

async fn worker_loop(
    redis_conn: MultiplexedConnection,
    events_collection: mongodb::Collection<EventLog>,
    guardian_collection: mongodb::Collection<GuardianLog>,
    metrics: Arc<Mutex<Metrics>>,
    cfg: Config,
) {
    loop {
        let mut did_work = false;

        // Clone connections for each task
        let mut conn1 = redis_conn.clone();
        let mut conn2 = redis_conn.clone();
        let mut conn3 = redis_conn.clone();
        let mut conn4 = redis_conn.clone();

        let (events_res, guardian_res) = tokio::join!(
            flush_logs::<EventLog>(
                &mut conn1,
                &events_collection,
                "logs:events",
                cfg.batch_size
            ),
            flush_logs::<GuardianLog>(
                &mut conn2,
                &guardian_collection,
                "logs:guardian",
                cfg.batch_size
            )
        );

        // Handle events result
        match events_res {
            Ok(count) if count > 0 => {
                did_work = true;
                let mut m = metrics.lock().unwrap();
                m.events_flushed += count;
                m.last_flush_ts = Utc::now().timestamp();
            }
            Ok(_) => {}
            Err(e) => {
                error!("Error flushing events: {:?}", e);
                let mut m = metrics.lock().unwrap();
                m.errors += 1;
            }
        }

        match guardian_res {
            Ok(count) if count > 0 => {
                did_work = true;
                let mut m = metrics.lock().unwrap();
                m.guardian_flushed += count;
                m.last_flush_ts = Utc::now().timestamp();
            }
            Ok(_) => {}
            Err(e) => {
                error!("Error flushing guardian logs: {:?}", e);
                let mut m = metrics.lock().unwrap();
                m.errors += 1;
            }
        }

        if did_work {
            info!("✅ Worker cycle complete, logs flushed");
        } else {
            info!("⏳ Worker cycle complete, no logs flushed this round");
        }

        let mut cmd_events = redis::cmd("LLEN");
        cmd_events.arg("logs:events");

        let mut cmd_guardian = redis::cmd("LLEN");
        cmd_guardian.arg("logs:guardian");

        let (events_len_res, guardian_len_res) = tokio::join!(
            cmd_events.query_async::<usize>(&mut conn3),
            cmd_guardian.query_async::<usize>(&mut conn4)
        );

        if let Ok(events_len) = events_len_res {
            let mut m = metrics.lock().unwrap();
            m.queue_events = events_len;
        }
        if let Ok(guardian_len) = guardian_len_res {
            let mut m = metrics.lock().unwrap();
            m.queue_guardian = guardian_len;
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
    // Wait for SIGINT or SIGTERM
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for shutdown signal");
    info!("🛑 Shutdown signal received, flushing remaining logs...");

    // Final flush before exit
    if let Ok(count) = flush_logs::<EventLog>(
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
    if let Ok(count) = flush_logs::<GuardianLog>(
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
