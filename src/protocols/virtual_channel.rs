//! Virtual channel for data aggregation and relay.
//!
//! A virtual channel does not connect to any physical device.
//! It serves as a data hub for aggregating data from multiple sources
//! or as an intermediate point for protocol conversion.
//!
//! # Architecture (No DataStore Dependency)
//!
//! `VirtualChannel` manages its own internal data buffer. Data is pushed
//! to it via `write()` and can be retrieved via `poll_once()`.
//!
//! # Example
//!
//! ```rust,ignore
//! use igw::protocols::virtual_channel::{VirtualChannel, VirtualChannelConfig};
//!
//! let config = VirtualChannelConfig::new("data_hub");
//! let mut channel = VirtualChannel::new(config);
//!
//! // Push data from any source
//! channel.write_point(DataPoint::telemetry(1, 25.5)).await?;
//!
//! // Get accumulated data (service layer handles storage)
//! let batch = channel.poll_once().await?;
//! store.write_batch(channel_id, &batch).await?;
//! ```

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{mpsc, RwLock};

use crate::core::data::{DataBatch, DataPoint};
use crate::core::error::Result;
use crate::core::metadata::{DriverMetadata, HasMetadata, ParameterMetadata, ParameterType};
use crate::core::point::PointConfig;
use crate::core::traits::{
    AdjustmentCommand, CommunicationMode, ConnectionState, ControlCommand, DataEvent,
    DataEventHandler, DataEventReceiver, DataEventSender, Diagnostics, EventDrivenProtocol,
    PollingConfig, Protocol, ProtocolCapabilities, ProtocolClient, ReadRequest, ReadResponse,
    WriteResult,
};

/// Virtual channel configuration.
#[derive(Debug, Clone)]
pub struct VirtualChannelConfig {
    /// Channel name for identification.
    pub name: String,

    /// Point configurations (defines accepted points).
    pub points: Vec<PointConfig>,

    /// Event buffer size.
    pub buffer_size: usize,
}

impl Default for VirtualChannelConfig {
    fn default() -> Self {
        Self {
            name: "virtual".to_string(),
            points: Vec::new(),
            buffer_size: 1024,
        }
    }
}

impl VirtualChannelConfig {
    /// Create a new virtual channel configuration.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Add point configurations.
    pub fn with_points(mut self, points: Vec<PointConfig>) -> Self {
        self.points = points;
        self
    }

    /// Set buffer size.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }
}

/// Virtual channel diagnostics.
#[derive(Debug, Default)]
struct VirtualDiagnostics {
    write_count: u64,
    read_count: u64,
    points_stored: usize,
}

/// Virtual channel implementation.
///
/// This channel type:
/// - Accepts data writes from any source via `write()` or `write_point()`
/// - Stores data internally (no external DataStore dependency)
/// - Emits events when data is written
/// - Returns accumulated data via `poll_once()`
pub struct VirtualChannel {
    config: VirtualChannelConfig,
    /// Internal data buffer: point_id -> DataPoint
    data_buffer: DashMap<u32, DataPoint>,
    diagnostics: Arc<RwLock<VirtualDiagnostics>>,
    event_tx: DataEventSender,
    _event_rx: Option<DataEventReceiver>,
    event_handler: Option<Arc<dyn DataEventHandler>>,
}

impl VirtualChannel {
    /// Create a new virtual channel.
    pub fn new(config: VirtualChannelConfig) -> Self {
        let (event_tx, event_rx) = mpsc::channel(config.buffer_size);

        Self {
            config,
            data_buffer: DashMap::new(),
            diagnostics: Arc::new(RwLock::new(VirtualDiagnostics::default())),
            event_tx,
            _event_rx: Some(event_rx),
            event_handler: None,
        }
    }

    /// Get the channel name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Write a data batch directly to this channel.
    ///
    /// This is the primary method for feeding data into a virtual channel.
    /// Data is stored internally and can be retrieved via `poll_once()`.
    pub async fn write(&self, batch: &DataBatch) -> Result<()> {
        // Store to internal buffer
        for point in batch.iter() {
            self.data_buffer.insert(point.id, point.clone());
        }

        // Emit event
        let _ = self
            .event_tx
            .send(DataEvent::DataUpdate(batch.clone()))
            .await;

        // Update diagnostics
        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += 1;
            diag.points_stored = self.data_buffer.len();
        }

        // Call event handler if set
        if let Some(handler) = &self.event_handler {
            handler.on_data_update(batch.clone()).await;
        }

        Ok(())
    }

    /// Write a single data point.
    pub async fn write_point(&self, point: DataPoint) -> Result<()> {
        let mut batch = DataBatch::new();
        batch.add(point);
        self.write(&batch).await
    }

    /// Get all points currently in the buffer.
    fn get_all_points(&self) -> DataBatch {
        let mut batch = DataBatch::new();
        for entry in self.data_buffer.iter() {
            batch.add(entry.value().clone());
        }
        batch
    }
}

impl HasMetadata for VirtualChannel {
    fn metadata() -> DriverMetadata {
        DriverMetadata {
            name: "virtual",
            display_name: "Virtual Channel",
            description: "Virtual channel for data aggregation, testing, and simulation. Does not connect to physical devices.",
            is_recommended: true,
            example_config: serde_json::json!({
                "name": "virtual_hub",
                "buffer_size": 1000,
                "mode": "aggregation"
            }),
            parameters: vec![
                ParameterMetadata::required(
                    "name",
                    "Name",
                    "Virtual channel name for identification",
                    ParameterType::String,
                ),
                ParameterMetadata::optional(
                    "buffer_size",
                    "Buffer Size",
                    "Maximum number of data points to buffer",
                    ParameterType::Integer,
                    serde_json::json!(1000),
                ),
                ParameterMetadata::optional(
                    "mode",
                    "Mode",
                    "Channel mode: 'aggregation' or 'simulation'",
                    ParameterType::String,
                    serde_json::json!("aggregation"),
                ),
            ],
        }
    }
}

impl ProtocolCapabilities for VirtualChannel {
    fn name(&self) -> &'static str {
        "Virtual"
    }

    fn supported_modes(&self) -> &[CommunicationMode] {
        &[CommunicationMode::EventDriven]
    }
}

impl Protocol for VirtualChannel {
    fn connection_state(&self) -> ConnectionState {
        // Virtual channels are always "connected"
        ConnectionState::Connected
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse> {
        // Read from internal buffer
        let batch = self.get_all_points();

        // Filter by request if needed
        let filtered = if let Some(ids) = &request.point_ids {
            let mut result = DataBatch::new();
            for point in batch.iter() {
                if ids.contains(&point.id) {
                    result.add(point.clone());
                }
            }
            result
        } else if let Some(data_type) = &request.data_type {
            let mut result = DataBatch::new();
            for point in batch.iter() {
                if &point.data_type == data_type {
                    result.add(point.clone());
                }
            }
            result
        } else {
            batch
        };

        {
            let mut diag = self.diagnostics.write().await;
            diag.read_count += 1;
        }

        Ok(ReadResponse::success(filtered))
    }

    async fn diagnostics(&self) -> Result<Diagnostics> {
        let diag = self.diagnostics.read().await;

        Ok(Diagnostics {
            protocol: "Virtual".to_string(),
            connection_state: ConnectionState::Connected,
            read_count: diag.read_count,
            write_count: diag.write_count,
            error_count: 0,
            last_error: None,
            extra: serde_json::json!({
                "name": self.config.name,
                "points_stored": diag.points_stored,
            }),
        })
    }
}

impl ProtocolClient for VirtualChannel {
    async fn connect(&mut self) -> Result<()> {
        // Virtual channel is always connected - no-op
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        // No-op for virtual channel
        Ok(())
    }

    /// Poll returns all data currently in the buffer.
    ///
    /// For virtual channels, data is pushed (not polled), so this returns
    /// the accumulated data. The service layer should call this to get
    /// data that was pushed via `write()`.
    async fn poll_once(&mut self) -> Result<DataBatch> {
        let batch = self.get_all_points();
        {
            let mut diag = self.diagnostics.write().await;
            diag.read_count += 1;
        }
        Ok(batch)
    }

    async fn write_control(&mut self, commands: &[ControlCommand]) -> Result<WriteResult> {
        // For virtual channels, control commands are stored as data points
        let mut batch = DataBatch::new();
        for cmd in commands {
            batch.add(DataPoint::control(cmd.id, cmd.value));
        }
        self.write(&batch).await?;
        Ok(WriteResult::success(commands.len()))
    }

    async fn write_adjustment(&mut self, adjustments: &[AdjustmentCommand]) -> Result<WriteResult> {
        // For virtual channels, adjustments are stored as data points
        let mut batch = DataBatch::new();
        for adj in adjustments {
            batch.add(DataPoint::adjustment(adj.id, adj.value));
        }
        self.write(&batch).await?;
        Ok(WriteResult::success(adjustments.len()))
    }

    async fn start_polling(&mut self, _config: PollingConfig) -> Result<()> {
        // No polling needed for virtual channels
        Ok(())
    }

    async fn stop_polling(&mut self) -> Result<()> {
        Ok(())
    }
}

impl EventDrivenProtocol for VirtualChannel {
    fn subscribe(&self) -> DataEventReceiver {
        // Create a new channel for each subscriber
        let (tx, rx) = mpsc::channel(self.config.buffer_size);
        // Note: In a real implementation, you'd want to use broadcast
        // or maintain a list of subscribers. This is simplified.
        let _ = tx; // Placeholder - actual impl would store tx
        rx
    }

    fn set_event_handler(&mut self, handler: Arc<dyn DataEventHandler>) {
        self.event_handler = Some(handler);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_virtual_channel_write_read() {
        let config = VirtualChannelConfig::new("test_channel");
        let channel = VirtualChannel::new(config);

        // Write a point
        channel
            .write_point(DataPoint::telemetry(1, 25.5))
            .await
            .unwrap();

        // Read it back
        let response = channel.read(ReadRequest::all()).await.unwrap();
        assert_eq!(response.data.len(), 1);

        let point = response.data.iter().next().unwrap();
        assert_eq!(point.id, 1);
    }

    #[tokio::test]
    async fn test_virtual_channel_always_connected() {
        let config = VirtualChannelConfig::new("test");
        let channel = VirtualChannel::new(config);

        assert_eq!(channel.connection_state(), ConnectionState::Connected);
    }

    #[tokio::test]
    async fn test_virtual_channel_poll_once() {
        let config = VirtualChannelConfig::new("poll_test");
        let mut channel = VirtualChannel::new(config);

        // Write some data
        channel
            .write_point(DataPoint::telemetry(1, 1.0))
            .await
            .unwrap();
        channel
            .write_point(DataPoint::telemetry(2, 2.0))
            .await
            .unwrap();

        // Poll returns accumulated data
        let batch = channel.poll_once().await.unwrap();
        assert_eq!(batch.len(), 2);
    }

    #[tokio::test]
    async fn test_virtual_channel_diagnostics() {
        let config = VirtualChannelConfig::new("diag_test");
        let channel = VirtualChannel::new(config);

        channel
            .write_point(DataPoint::telemetry(1, 1.0))
            .await
            .unwrap();
        channel
            .write_point(DataPoint::telemetry(2, 2.0))
            .await
            .unwrap();

        let diag = channel.diagnostics().await.unwrap();
        assert_eq!(diag.write_count, 2);
        assert_eq!(diag.protocol, "Virtual");
    }
}
