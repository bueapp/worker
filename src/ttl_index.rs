use bson::doc;
use mongodb::IndexModel;
use mongodb::options::IndexOptions;

use crate::models::{EventLog, GuardianLog, JoinLog};

pub async fn ensure_ttl_indexes(
    events_collection: &mongodb::Collection<EventLog>,
    guardian_collection: &mongodb::Collection<GuardianLog>,
    join_collection: &mongodb::Collection<JoinLog>,
) -> mongodb::error::Result<()> {
    let events_index = IndexModel::builder()
        .keys(doc! { "loggedAt": 1 })
        .options(
            IndexOptions::builder()
                .expire_after(Some(std::time::Duration::from_secs(60 * 60 * 24 * 14)))
                .build(),
        )
        .build();
    events_collection.create_index(events_index).await?;

    let guardian_index = IndexModel::builder()
        .keys(doc! { "createdAt": 1 })
        .options(
            IndexOptions::builder()
                .expire_after(Some(std::time::Duration::from_secs(60 * 60 * 24 * 14)))
                .build(),
        )
        .build();
    guardian_collection.create_index(guardian_index).await?;

    let join_index = IndexModel::builder()
        .keys(doc! { "createdAt": 1 })
        .options(
            IndexOptions::builder()
                .expire_after(Some(std::time::Duration::from_secs(60 * 60 * 24 * 3)))
                .build(),
        )
        .build();

    join_collection.create_index(join_index).await?;

    Ok(())
}
