use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub redis_url: String,
    pub mongo_url: String,
    pub mongo_db: String,
    pub metrics_port: u16,
    pub flush_interval: u64,
    pub batch_size: usize,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        Self {
            redis_url: env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string()),
            mongo_url: env::var("MONGO_URL")
                .unwrap_or_else(|_| "mongodb://127.0.0.1:27017".to_string()),
            mongo_db: env::var("MONGO_DB").unwrap_or_else(|_| "discord_logs".to_string()),
            metrics_port: env::var("METRICS_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
            flush_interval: env::var("FLUSH_INTERVAL")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            batch_size: env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "1000".to_string())
                .parse()
                .unwrap_or(1000),
        }
    }
}
