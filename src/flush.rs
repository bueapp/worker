use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bson::spec::BinarySubtype;
use bson::{Binary, Document, doc};
use mongodb::Collection;
use redis::aio::MultiplexedConnection;
use std::io::Cursor;
use zstd::encode_all;

pub async fn flush_logs(
    redis_conn: &mut MultiplexedConnection,
    collection: &Collection<Document>,
    redis_key: &str,
    batch_size: usize,
) -> anyhow::Result<usize> {
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

    let mut parsed_logs: Vec<Document> = Vec::new();

    for s in logs {
        if let Ok(bytes) = BASE64.decode(s) {
            if let Ok(doc) = bson::from_slice::<Document>(&bytes) {
                if let Ok(serialized) = bson::to_vec(&doc) {
                    if let Ok(compressed) = encode_all(Cursor::new(serialized), 3) {
                        parsed_logs.push(doc! {
                            "compressed": Binary {
                                subtype: BinarySubtype::Generic,
                                bytes: compressed
                            },
                            "ts": chrono::Utc::now().timestamp_millis()
                        });
                    }
                }
            }
        }
    }

    if parsed_logs.is_empty() {
        return Ok(0);
    }

    let insert_result = collection.insert_many(parsed_logs.clone()).await;

    if let Err(e) = insert_result {
        let mut dlq_conn = redis_conn.clone();
        for doc in parsed_logs {
            if let Ok(bytes) = bson::to_vec(&doc) {
                let base64 = BASE64.encode(&bytes);
                let _ = redis::cmd("LPUSH")
                    .arg("logs:failed")
                    .arg(base64)
                    .query_async::<()>(&mut dlq_conn)
                    .await;
            }
        }
        tracing::error!(error=?e, "❌ Failed to insert logs, sent to DLQ");
        return Err(anyhow::anyhow!(e));
    }

    Ok(parsed_logs.len())
}
