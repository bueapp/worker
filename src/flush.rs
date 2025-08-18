use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bson::{Document, from_slice};
use mongodb::Collection;
use redis::aio::MultiplexedConnection;
use tokio::time::{Duration, sleep};
use tracing::info;

async fn retry_with_backoff<F, T, E>(mut f: F, max_retries: u32) -> Result<T, E>
where
    F: FnMut() -> futures::future::BoxFuture<'static, Result<T, E>>,
    E: std::fmt::Debug + std::fmt::Display,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 0..max_retries {
        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                if attempt + 1 == max_retries {
                    return Err(e);
                }
                eprintln!("Attempt {} failed: {}", attempt + 1, e);
                sleep(delay).await;
                delay *= 2; // exponential backoff
            }
        }
    }
    unreachable!()
}

pub async fn flush_logs(
    redis_conn: &mut MultiplexedConnection,
    collection: &Collection<Document>,
    redis_key: &str,
    batch_size: usize,
) -> Result<usize> {
    let logs: Vec<Vec<u8>> = redis::cmd("LRANGE")
        .arg(redis_key)
        .arg(0)
        .arg((batch_size - 1) as isize)
        .query_async(redis_conn)
        .await?;

    if logs.is_empty() {
        return Ok(0);
    }

    redis::cmd("LTRIM")
        .arg(redis_key)
        .arg(logs.len())
        .arg(-1)
        .query_async::<()>(redis_conn)
        .await?;

    let parsed_logs: Vec<Document> = logs
        .into_iter()
        .filter_map(|s| {
            let bytes = BASE64.decode(s).ok()?;
            from_slice::<Document>(&bytes).ok()
        })
        .collect();

    if !parsed_logs.is_empty() {
        let insert_result = retry_with_backoff(
            || {
                let collection = collection.clone();
                let docs = parsed_logs.clone();
                Box::pin(async move { collection.insert_many(docs).await })
            },
            5, // max retries
        )
        .await;

        match insert_result {
            Ok(_) => info!("Successfully flushed {} logs to MongoDB", parsed_logs.len()),
            Err(e) => {
                tracing::error!("Failed to flush logs to MongoDB: {}", e);
                return Err(e.into());
            }
        }
    }

    Ok(parsed_logs.len())
}
