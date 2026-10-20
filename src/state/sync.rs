//! A suppression page and its resume token commit together.

use anyhow::ensure;
use sqlx::Row;

use super::{Store, Suppression, merge_all, validate};

#[derive(Debug)]
pub struct Checkpoint {
    pub next_token: Option<String>,
    pub completed: bool,
    pub imported: u64,
}

impl Store {
    pub async fn checkpoint(&self, source: &str) -> anyhow::Result<Option<Checkpoint>> {
        sqlx::query(
            "SELECT next_token, completed, imported FROM hailer.sync_checkpoints WHERE source = $1",
        )
        .bind(source)
        .fetch_optional(&self.pool)
        .await?
        .map(|row| {
            Ok(Checkpoint {
                next_token: row.try_get("next_token")?,
                completed: row.try_get("completed")?,
                imported: u64::try_from(row.try_get::<i64, _>("imported")?)?,
            })
        })
        .transpose()
    }

    pub async fn start_sync(&self, source: &str, restart: bool) -> anyhow::Result<Checkpoint> {
        let mut transaction = self.write().await?;
        sqlx::query(
            "INSERT INTO hailer.sync_checkpoints(source) VALUES ($1) \
             ON CONFLICT (source) DO UPDATE SET next_token = NULL, completed = false, imported = 0, \
                updated_at = clock_timestamp() \
             WHERE hailer.sync_checkpoints.completed OR $2",
        )
        .bind(source).bind(restart).execute(&mut *transaction).await?;
        transaction.commit().await?;
        self.checkpoint(source).await?.ok_or_else(|| anyhow::anyhow!("sync checkpoint missing"))
    }

    pub async fn import_page(
        &self,
        source: &str,
        expected_token: Option<&str>,
        records: Vec<Suppression>,
        next_token: Option<&str>,
    ) -> anyhow::Result<Checkpoint> {
        ensure!(records.len() <= 1000, "suppression page exceeds 1000 entries");
        let records = records.into_iter().map(validate).collect::<anyhow::Result<Vec<_>>>()?;
        let mut transaction = self.write().await?;
        let row = sqlx::query("SELECT next_token, completed FROM hailer.sync_checkpoints WHERE source = $1 FOR UPDATE")
            .bind(source).fetch_one(&mut *transaction).await?;
        let current: Option<String> = row.try_get("next_token")?;
        ensure!(
            !row.try_get::<bool, _>("completed")? && current.as_deref() == expected_token,
            "sync checkpoint changed"
        );
        merge_all(&mut transaction, &records).await?;
        sqlx::query(
            "UPDATE hailer.sync_checkpoints SET next_token = $2, completed = ($2::text IS NULL), \
             imported = imported + $3, updated_at = clock_timestamp(), \
             completed_at = CASE WHEN $2::text IS NULL THEN clock_timestamp() ELSE completed_at END \
             WHERE source = $1",
        )
        .bind(source).bind(next_token).bind(i64::try_from(records.len())?)
        .execute(&mut *transaction).await?;
        transaction.commit().await?;
        self.checkpoint(source).await?.ok_or_else(|| anyhow::anyhow!("sync checkpoint missing"))
    }
}
