use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct EventLog {
    pub guildId: String,
    pub action: String,
    pub loggedAt: i64,
    pub actionThreadId: Option<String>,
    pub values: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GuardianLog {
    pub guildId: String,
    pub authorId: String,
    pub messageId: String,
    pub content: String,
    pub categories: Value,
    pub flagged: bool,
    #[serde(default)]
    pub createdAt: Option<i64>,
}
