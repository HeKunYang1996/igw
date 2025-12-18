//! DataStore trait definition.

use async_trait::async_trait;

use crate::core::data::{DataBatch, DataPoint};
use crate::core::error::Result;
use crate::core::point::PointConfig;
use crate::core::traits::DataEventReceiver;

/// Trait for data storage backends.
///
/// This trait abstracts the storage layer, allowing the gateway to work
/// with different backends (memory, Redis, etc.) without changing the
/// protocol implementation.
#[async_trait]
pub trait DataStore: Send + Sync {
    /// Write a batch of data points for a channel.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    /// * `batch` - The data batch to write
    async fn write_batch(&self, channel_id: u32, batch: &DataBatch) -> Result<()>;

    /// Read a specific data point.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    /// * `point_id` - The point identifier
    async fn read_point(&self, channel_id: u32, point_id: &str) -> Result<Option<DataPoint>>;

    /// Read all data points for a channel.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    async fn read_all(&self, channel_id: u32) -> Result<DataBatch>;

    /// Subscribe to data change events.
    ///
    /// Returns a receiver that will receive `DataEvent` notifications
    /// whenever data is written to the store.
    fn subscribe(&self) -> DataEventReceiver;

    /// Get point configuration for a specific point.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    /// * `point_id` - The point identifier
    fn get_point_config(&self, channel_id: u32, point_id: &str) -> Option<PointConfig>;

    /// Set point configurations for a channel.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    /// * `configs` - The point configurations to set
    fn set_point_configs(&self, channel_id: u32, configs: Vec<PointConfig>);

    /// Get all point configurations for a channel.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    fn get_all_point_configs(&self, channel_id: u32) -> Vec<PointConfig>;

    /// Clear all data for a channel.
    ///
    /// # Arguments
    ///
    /// * `channel_id` - The channel identifier
    async fn clear_channel(&self, channel_id: u32) -> Result<()>;
}
