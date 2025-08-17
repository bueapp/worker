use anyhow::Result;
use mongodb::Collection;
use redis::aio::MultiplexedConnection;
use serde::{Deserialize, Serialize};
use tracing::info;

pub async fn flush_logs<T>(
    redis_conn: &mut MultiplexedConnection,
    collection: &Collection<T>,
    redis_key: &str,
    batch_size: usize,
) -> Result<usize>
where
    T: for<'de> Deserialize<'de> + Serialize + Unpin + Send + Sync,
{
    let logs: Vec<String> = redis::cmd("LRANGE")
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

    let parsed_logs: Vec<T> = logs
        .into_iter()
        .filter_map(|log| serde_json::from_str(&log).ok())
        .collect();

    if !parsed_logs.is_empty() {
        collection.insert_many(&parsed_logs).await?;
        info!("✅ Flushed {} logs from {}", parsed_logs.len(), redis_key);
    }

    Ok(parsed_logs.len())
}
