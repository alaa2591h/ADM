use adm_types::{AppConfig, DownloadChunk, DownloadTask};
use anyhow::Result;
use async_trait::async_trait;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::path::Path;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWriteRange {
    pub id: i64,
    pub task_id: Uuid,
    pub start: u64,
    pub end: u64,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedWriteReservation {
    pub id: Uuid,
    pub task_id: Uuid,
    pub chunk_id: Uuid,
    pub offset: u64,
    pub length: u64,
    pub state: String,
    pub reserved_at: i64,
    pub committed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Default)]
pub struct ShutdownToken;
impl ShutdownToken {
    pub async fn cancelled(&self) {}
    pub fn is_cancelled(&self) -> bool {
        false
    }
}

pub struct Storage {
    pool: r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>,
}

impl Storage {
    /// Opens a `SQLite` database at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let manager = r2d2_sqlite::SqliteConnectionManager::file(path);
        let pool = r2d2::Pool::new(manager)?;
        let storage = Self { pool };
        storage.init_db()?;
        Ok(storage)
    }

    fn init_db(&self) -> Result<()> {
        let conn = self.pool.get()?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS downloads (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                state TEXT NOT NULL,
                filename TEXT NOT NULL,
                save_path TEXT NOT NULL,
                total_bytes INTEGER,
                downloaded_bytes INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                headers TEXT,
                speed_limit_kbps INTEGER,
                checksum_retry_count INTEGER
            )",
            [],
        )?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;
        Ok(())
    }

    pub fn is_shutdown(&self) -> bool {
        false
    }

    // --- Task Persistence ---

    pub async fn save_task(&self, task: DownloadTask) -> Result<()> {
        let pool = self.pool.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let conn = pool.get()?;
            let headers = serde_json::to_string(&task.headers)?;
            let state = serde_json::to_string(&task.state)?;
            let filename = task.filename.as_deref().unwrap_or("");
            let save_path = task
                .save_path
                .as_ref()
                .map_or_else(|| "".to_string(), |p| p.to_string_lossy().to_string());
            conn.execute(
                "REPLACE INTO downloads (id, url, state, filename, save_path, total_bytes, downloaded_bytes, created_at, headers, speed_limit_kbps, checksum_retry_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    task.id.to_string(),
                    task.url,
                    state,
                    filename,
                    save_path,
                    task.total_bytes.map(|v| v as i64),
                    task.downloaded_bytes as i64,
                    task.created_at.duration_since(std::time::UNIX_EPOCH)?.as_secs() as i64,
                    headers,
                    task.speed_limit_kbps.map(|v| v as i64),
                    task.checksum_retry_count,
                ],
            )?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    pub async fn load_task(&self, _id: Uuid) -> Result<Option<DownloadTask>> {
        Ok(None)
    }

    pub async fn load_pending_tasks(&self) -> Result<Vec<DownloadTask>> {
        Ok(vec![])
    }

    // --- Config Persistence ---

    pub async fn load_config(&self) -> Result<AppConfig> {
        let pool = self.pool.clone();
        let res = tokio::task::spawn_blocking(move || -> Result<Option<String>> {
            let conn = pool.get()?;
            let mut stmt = conn.prepare("SELECT value FROM config WHERE key = 'app_config'")?;
            let res = stmt.query_row([], |row| {
                let val: String = row.get(0)?;
                Ok(val)
            });
            match res {
                Ok(val) => Ok(Some(val)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(e.into()),
            }
        })
        .await??;

        match res {
            Some(val) => Ok(serde_json::from_str::<AppConfig>(&val)?),
            None => {
                let default = AppConfig::default();
                self.save_config(&default).await?;
                Ok(default)
            }
        }
    }

    pub async fn save_config(&self, config: &AppConfig) -> Result<()> {
        let conn = self.pool.get()?;
        conn.execute(
            "REPLACE INTO config (key, value) VALUES ('app_config', ?1)",
            params![serde_json::to_string(config)?],
        )?;
        Ok(())
    }

    // --- Write Reservations & Ranges ---

    pub async fn load_write_reservations_for_task(
        &self,
        _task_id: Uuid,
    ) -> Result<Vec<PersistedWriteReservation>> {
        Ok(vec![])
    }

    pub async fn load_write_ranges_for_task(
        &self,
        _task_id: Uuid,
    ) -> Result<Vec<PersistedWriteRange>> {
        Ok(vec![])
    }

    pub async fn delete_write_ranges_in_range(
        &self,
        _task_id: Uuid,
        _state: &str,
        _start: u64,
        _end: u64,
    ) -> Result<()> {
        Ok(())
    }

    pub async fn save_write_reservation(
        &self,
        _reservation: PersistedWriteReservation,
    ) -> Result<()> {
        Ok(())
    }

    pub async fn save_write_range(&self, _range: PersistedWriteRange) -> Result<()> {
        Ok(())
    }

    pub async fn save_snapshot(
        &self,
        _name: &str,
        _payload: &serde_json::Value,
        _compress: bool,
    ) -> Result<()> {
        Ok(())
    }

    pub async fn delete_chunks_for_task(&self, _task_id: Uuid) -> Result<()> {
        Ok(())
    }

    pub async fn delete_write_reservations_for_task(&self, _task_id: Uuid) -> Result<()> {
        Ok(())
    }

    pub async fn delete_write_ranges_for_task(&self, _task_id: Uuid) -> Result<()> {
        Ok(())
    }

    pub async fn load_chunks_for_task(&self, _task_id: Uuid) -> Result<Vec<DownloadChunk>> {
        Ok(vec![])
    }

    pub async fn load_pending_chunks(&self) -> Result<Vec<DownloadChunk>> {
        Ok(vec![])
    }

    pub async fn recover_orphaned_chunks(&self) -> Result<()> {
        Ok(())
    }

    pub async fn save_chunk(&self, _chunk: DownloadChunk) -> Result<()> {
        Ok(())
    }

    pub async fn append_history(
        &self,
        _task_id: Uuid,
        _event: &str,
        _timestamp: i64,
    ) -> Result<()> {
        Ok(())
    }
}

pub trait SnapshotRepository: Send + Sync {}
impl SnapshotRepository for Storage {}

#[async_trait]
pub trait ChunkRepository: Send + Sync {
    async fn load_pending_chunks(&self) -> Result<Vec<DownloadChunk>>;
}
#[async_trait]
impl ChunkRepository for Storage {
    async fn load_pending_chunks(&self) -> Result<Vec<DownloadChunk>> {
        Self::load_pending_chunks(self).await
    }
}

pub trait HistoryRepository: Send + Sync {}
impl HistoryRepository for Storage {}

pub trait TaskRepository: Send + Sync {}
impl TaskRepository for Storage {}
