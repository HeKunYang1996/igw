//! In-memory data store implementation using DashMap.

use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use crate::core::data::{DataBatch, DataPoint};
use crate::core::error::Result;
use crate::core::point::PointConfig;
use crate::core::traits::{DataEvent, DataEventReceiver, DataEventSender};

use super::traits::DataStore;

/// In-memory data store using DashMap for concurrent access.
///
/// This is the default storage backend for standalone usage.
/// It provides fast, lock-free reads and writes without external dependencies.
///
/// # Example
///
/// ```rust
/// use igw::store::MemoryStore;
///
/// let store = MemoryStore::new();
/// ```
pub struct MemoryStore {
    /// Data storage: "channel_id:point_id" -> DataPoint
    data: DashMap<String, DataPoint>,

    /// Point configurations: channel_id -> Vec<PointConfig>
    configs: DashMap<u32, Vec<PointConfig>>,

    /// Event subscribers
    subscribers: Arc<RwLock<Vec<DataEventSender>>>,
}

impl MemoryStore {
    /// Create a new memory store.
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
            configs: DashMap::new(),
            subscribers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Generate a storage key for a data point.
    fn make_key(channel_id: u32, point_id: &str) -> String {
        format!("{}:{}", channel_id, point_id)
    }

    /// Notify all subscribers of a data event.
    async fn notify_subscribers(&self, event: DataEvent) {
        let subscribers = self.subscribers.read().await;
        for sender in subscribers.iter() {
            // Best-effort delivery, ignore errors
            let _ = sender.try_send(event.clone());
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DataStore for MemoryStore {
    async fn write_batch(&self, channel_id: u32, batch: &DataBatch) -> Result<()> {
        // Write telemetry points
        for point in &batch.telemetry {
            let key = Self::make_key(channel_id, &point.id);
            self.data.insert(key, point.clone());
        }

        // Write signal points
        for point in &batch.signal {
            let key = Self::make_key(channel_id, &point.id);
            self.data.insert(key, point.clone());
        }

        // Write control points
        for point in &batch.control {
            let key = Self::make_key(channel_id, &point.id);
            self.data.insert(key, point.clone());
        }

        // Write adjustment points
        for point in &batch.adjustment {
            let key = Self::make_key(channel_id, &point.id);
            self.data.insert(key, point.clone());
        }

        // Notify subscribers
        if !batch.is_empty() {
            self.notify_subscribers(DataEvent::DataUpdate(batch.clone()))
                .await;
        }

        Ok(())
    }

    async fn read_point(&self, channel_id: u32, point_id: &str) -> Result<Option<DataPoint>> {
        let key = Self::make_key(channel_id, point_id);
        Ok(self.data.get(&key).map(|r| r.value().clone()))
    }

    async fn read_all(&self, channel_id: u32) -> Result<DataBatch> {
        let prefix = format!("{}:", channel_id);
        let mut batch = DataBatch::default();

        for entry in self.data.iter() {
            if entry.key().starts_with(&prefix) {
                let point = entry.value().clone();
                batch.add(point);
            }
        }

        Ok(batch)
    }

    fn subscribe(&self) -> DataEventReceiver {
        let (tx, rx) = mpsc::channel(1024);

        // Clone Arc and spawn a task to add the subscriber
        let subscribers = self.subscribers.clone();
        tokio::spawn(async move {
            let mut subs = subscribers.write().await;
            subs.push(tx);
        });

        rx
    }

    fn get_point_config(&self, channel_id: u32, point_id: &str) -> Option<PointConfig> {
        self.configs.get(&channel_id).and_then(|configs| {
            configs
                .value()
                .iter()
                .find(|c| c.id == point_id)
                .cloned()
        })
    }

    fn set_point_configs(&self, channel_id: u32, configs: Vec<PointConfig>) {
        self.configs.insert(channel_id, configs);
    }

    fn get_all_point_configs(&self, channel_id: u32) -> Vec<PointConfig> {
        self.configs
            .get(&channel_id)
            .map(|c| c.value().clone())
            .unwrap_or_default()
    }

    async fn clear_channel(&self, channel_id: u32) -> Result<()> {
        let prefix = format!("{}:", channel_id);

        // Collect keys to remove
        let keys_to_remove: Vec<String> = self
            .data
            .iter()
            .filter(|e| e.key().starts_with(&prefix))
            .map(|e| e.key().clone())
            .collect();

        // Remove them
        for key in keys_to_remove {
            self.data.remove(&key);
        }

        // Clear configs
        self.configs.remove(&channel_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::data::DataType;
    use crate::core::point::ProtocolAddress;

    #[tokio::test]
    async fn test_memory_store_write_read() {
        let store = MemoryStore::new();

        // Create a batch with some points
        let mut batch = DataBatch::default();
        batch.add(DataPoint::telemetry("temp", 25.5));
        batch.add(DataPoint::signal("door", true));

        // Write
        store.write_batch(1, &batch).await.unwrap();

        // Read back
        let point = store.read_point(1, "temp").await.unwrap();
        assert!(point.is_some());
        assert_eq!(point.unwrap().value.as_f64(), Some(25.5));

        // Read all
        let all = store.read_all(1).await.unwrap();
        assert_eq!(all.telemetry.len(), 1);
        assert_eq!(all.signal.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_store_configs() {
        let store = MemoryStore::new();

        // Set configs
        let configs = vec![PointConfig::new(
            "temp",
            DataType::Telemetry,
            ProtocolAddress::Generic("test".to_string()),
        )];
        store.set_point_configs(1, configs);

        // Get config
        let config = store.get_point_config(1, "temp");
        assert!(config.is_some());
        assert_eq!(config.unwrap().id, "temp");

        // Get all configs
        let all = store.get_all_point_configs(1);
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn test_memory_store_clear() {
        let store = MemoryStore::new();

        // Write some data
        let mut batch = DataBatch::default();
        batch.add(DataPoint::telemetry("temp", 25.5));
        store.write_batch(1, &batch).await.unwrap();

        // Clear
        store.clear_channel(1).await.unwrap();

        // Verify empty
        let all = store.read_all(1).await.unwrap();
        assert!(all.is_empty());
    }
}
