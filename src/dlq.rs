use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bson::{Document, from_slice};
use mongodb::Collection;
use redis::aio::MultiplexedConnection;
use tokio::time::{Duration, sleep};
use tracing::{error, info};

pub async fn dlq_reprocessor(
    mut redis_conn: MultiplexedConnection,
    collection: Collection<Document>,
    batch_size: usize,
) -> Result<()> {
    loop {
        let logs: Vec<String> = redis::cmd("LRANGE")
            .arg("logs:failed")
            .arg(0)
            .arg((batch_size - 1) as isize)
            .query_async(&mut redis_conn)
            .await?;

        if logs.is_empty() {
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        redis::cmd("LTRIM")
            .arg("logs:failed")
            .arg(logs.len())
            .arg(-1)
            .query_async::<()>(&mut redis_conn)
            .await?;

        let mut parsed_logs: Vec<Document> = Vec::new();

        for s in logs {
            if let Ok(bytes) = BASE64.decode(s) {
                if let Ok(doc) = from_slice::<Document>(&bytes) {
                    parsed_logs.push(doc);
                }
            }
        }

        if parsed_logs.is_empty() {
            continue;
        }

        match collection.insert_many(parsed_logs.clone()).await {
            Ok(_) => {
                info!(count = parsed_logs.len(), "✅ Reprocessed logs from DLQ");
            }
            Err(e) => {
                error!(error=?e, "❌ Failed to reprocess logs, pushing back to DLQ");
                for doc in parsed_logs {
                    if let Ok(bytes) = bson::to_vec(&doc) {
                        let base64 = BASE64.encode(&bytes);
                        let _ = redis::cmd("LPUSH")
                            .arg("logs:failed")
                            .arg(base64)
                            .query_async::<()>(&mut redis_conn)
                            .await;
                    }
                }
            }
        }
    }
}

