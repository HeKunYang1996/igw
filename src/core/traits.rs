//! Core traits for protocol implementations.
//!
//! This module defines the fundamental traits that all protocols must implement.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::core::data::{DataBatch, DataType};
use crate::core::error::Result;

/// Communication mode supported by a protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommunicationMode {
    /// Polling mode - actively request data at intervals.
    ///
    /// Used by: Modbus, BACnet (read), etc.
    Polling,

    /// Event-driven mode - passively receive data updates.
    ///
    /// Used by: IEC 104 (spontaneous), OPC UA (subscriptions), etc.
    EventDriven,

    /// Hybrid mode - supports both polling and events.
    ///
    /// Used by: DNP3, OPC UA, etc.
    Hybrid,
}

/// Connection state of a protocol client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    /// Not connected to the target.
    #[default]
    Disconnected,

    /// Attempting to connect.
    Connecting,

    /// Connected and operational.
    Connected,

    /// Attempting to reconnect after failure.
    Reconnecting,

    /// Connection error state.
    Error,
}

impl ConnectionState {
    /// Check if currently connected.
    #[inline]
    pub const fn is_connected(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Check if retry is possible.
    #[inline]
    pub const fn can_retry(&self) -> bool {
        matches!(self, Self::Disconnected | Self::Error)
    }
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Disconnected => "Disconnected",
            Self::Connecting => "Connecting",
            Self::Connected => "Connected",
            Self::Reconnecting => "Reconnecting",
            Self::Error => "Error",
        };
        write!(f, "{}", s)
    }
}

/// Request for reading data points.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    /// Data type to read (None = all types)
    pub data_type: Option<DataType>,

    /// Point IDs to read (None = all points)
    pub point_ids: Option<Vec<u32>>,
}

impl ReadRequest {
    /// Create a request for all points of a specific type.
    pub fn by_type(data_type: DataType) -> Self {
        Self {
            data_type: Some(data_type),
            point_ids: None,
        }
    }

    /// Create a request for specific points.
    pub fn by_ids(ids: Vec<u32>) -> Self {
        Self {
            data_type: None,
            point_ids: Some(ids),
        }
    }

    /// Create a request for all telemetry points.
    pub fn telemetry() -> Self {
        Self::by_type(DataType::Telemetry)
    }

    /// Create a request for all signal points.
    pub fn signal() -> Self {
        Self::by_type(DataType::Signal)
    }

    /// Create a request for all points.
    pub fn all() -> Self {
        Self {
            data_type: None,
            point_ids: None,
        }
    }
}

/// Response from reading data points.
#[derive(Debug, Clone)]
pub struct ReadResponse {
    /// The data batch containing all read points.
    pub data: DataBatch,

    /// Number of points that failed to read.
    pub failed_count: usize,

    /// Detailed partial read failures: (point_id, error_message).
    ///
    /// This allows callers to know exactly which points failed and why,
    /// rather than just a count.
    pub partial_errors: Vec<(u32, String)>,
}

impl ReadResponse {
    /// Create a successful response with no errors.
    pub fn success(data: DataBatch) -> Self {
        Self {
            data,
            failed_count: 0,
            partial_errors: Vec::new(),
        }
    }

    /// Create a response with partial failures (count only, for backward compat).
    pub fn partial(data: DataBatch, failed: usize) -> Self {
        Self {
            data,
            failed_count: failed,
            partial_errors: Vec::new(),
        }
    }

    /// Create a response with detailed error information.
    pub fn with_errors(data: DataBatch, errors: Vec<(u32, String)>) -> Self {
        let failed_count = errors.len();
        Self {
            data,
            failed_count,
            partial_errors: errors,
        }
    }

    /// Check if any reads failed.
    ///
    /// Returns true if either:
    /// - `failed_count > 0` (from partial() or with_errors())
    /// - `partial_errors` is not empty
    pub fn has_errors(&self) -> bool {
        self.failed_count > 0 || !self.partial_errors.is_empty()
    }

    /// Get a summary of errors suitable for logging.
    ///
    /// Returns `Some((total_count, first_few_errors))` if there are errors,
    /// where first_few_errors contains at most 3 error messages.
    /// Returns `None` if no errors.
    pub fn error_summary(&self) -> Option<(usize, Vec<&str>)> {
        if !self.has_errors() {
            return None;
        }

        let count = self.failed_count.max(self.partial_errors.len());
        let first_few: Vec<&str> = self
            .partial_errors
            .iter()
            .take(3)
            .map(|(_, msg)| msg.as_str())
            .collect();

        Some((count, first_few))
    }
}

/// A control command to write.
#[derive(Debug, Clone)]
pub struct ControlCommand {
    /// Point ID
    pub id: u32,

    /// Command value (true = ON/CLOSE, false = OFF/OPEN)
    pub value: bool,

    /// Pulse duration in milliseconds (None = latching)
    pub pulse_duration_ms: Option<u32>,
}

impl ControlCommand {
    /// Create a latching control command.
    pub fn latching(id: u32, value: bool) -> Self {
        Self {
            id,
            value,
            pulse_duration_ms: None,
        }
    }

    /// Create a pulse control command.
    pub fn pulse(id: u32, value: bool, duration_ms: u32) -> Self {
        Self {
            id,
            value,
            pulse_duration_ms: Some(duration_ms),
        }
    }
}

/// An adjustment command to write.
#[derive(Debug, Clone)]
pub struct AdjustmentCommand {
    /// Point ID
    pub id: u32,

    /// Setpoint value
    pub value: f64,
}

impl AdjustmentCommand {
    /// Create an adjustment command.
    pub fn new(id: u32, value: f64) -> Self {
        Self { id, value }
    }
}

/// Result of write operations.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of successful writes.
    pub success_count: usize,

    /// IDs of failed writes with error messages.
    pub failures: Vec<(u32, String)>,
}

impl WriteResult {
    /// Create a fully successful result.
    pub fn success(count: usize) -> Self {
        Self {
            success_count: count,
            failures: vec![],
        }
    }

    /// Check if all writes succeeded.
    pub fn is_success(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Polling configuration.
#[derive(Debug, Clone)]
pub struct PollingConfig {
    /// Polling interval in milliseconds.
    pub interval_ms: u64,

    /// Data types to poll (None = all).
    pub data_types: Option<Vec<DataType>>,

    /// Whether to continue on individual point errors.
    pub continue_on_error: bool,
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            interval_ms: 1000,
            data_types: None,
            continue_on_error: true,
        }
    }
}

/// Protocol diagnostics information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Diagnostics {
    /// Protocol name.
    pub protocol: String,

    /// Connection state.
    pub connection_state: ConnectionState,

    /// Number of successful reads.
    pub read_count: u64,

    /// Number of successful writes.
    pub write_count: u64,

    /// Number of errors.
    pub error_count: u64,

    /// Last error message.
    pub last_error: Option<String>,

    /// Protocol-specific information.
    #[serde(default)]
    pub extra: serde_json::Value,
}

impl Diagnostics {
    /// Create new diagnostics.
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            connection_state: ConnectionState::Disconnected,
            read_count: 0,
            write_count: 0,
            error_count: 0,
            last_error: None,
            extra: serde_json::Value::Null,
        }
    }
}

/// Protocol capabilities description.
pub trait ProtocolCapabilities {
    /// Get the protocol name.
    fn name(&self) -> &'static str;

    /// Get supported communication modes.
    fn supported_modes(&self) -> &[CommunicationMode];

    /// Check if client role is supported.
    fn supports_client(&self) -> bool {
        true
    }

    /// Check if server role is supported.
    fn supports_server(&self) -> bool {
        false
    }

    /// Get protocol version.
    fn version(&self) -> &'static str {
        "1.0"
    }
}

/// Base protocol trait - read-only operations.
pub trait Protocol: ProtocolCapabilities + Send + Sync {
    /// Get current connection state.
    fn connection_state(&self) -> ConnectionState;

    /// Read data points.
    fn read(&self, request: ReadRequest) -> impl Future<Output = Result<ReadResponse>> + Send;

    /// Get diagnostics information.
    fn diagnostics(&self) -> impl Future<Output = Result<Diagnostics>> + Send;
}

/// Client protocol trait - active connection + write operations.
pub trait ProtocolClient: Protocol {
    /// Connect to the target device/server.
    fn connect(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Disconnect from the target.
    fn disconnect(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Execute a single poll cycle and return collected data.
    ///
    /// This is the primary method for data acquisition. The caller (service layer)
    /// is responsible for storing the returned data. The protocol layer only
    /// handles device communication.
    ///
    /// # Returns
    ///
    /// A `DataBatch` containing all successfully read points from configured sources.
    fn poll_once(&mut self) -> impl Future<Output = Result<DataBatch>> + Send;

    /// Write control commands.
    fn write_control(
        &mut self,
        commands: &[ControlCommand],
    ) -> impl Future<Output = Result<WriteResult>> + Send;

    /// Write adjustment commands.
    fn write_adjustment(
        &mut self,
        adjustments: &[AdjustmentCommand],
    ) -> impl Future<Output = Result<WriteResult>> + Send;

    /// Start polling task (legacy, prefer using poll_once() with external loop).
    ///
    /// This method is kept for backward compatibility. New implementations
    /// should use `poll_once()` with an external polling loop managed by the service layer.
    fn start_polling(&mut self, config: PollingConfig) -> impl Future<Output = Result<()>> + Send;

    /// Stop polling task.
    fn stop_polling(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Server protocol trait - passive connection acceptance.
pub trait ProtocolServer: Protocol {
    /// Start listening on the specified address.
    fn listen(&mut self, addr: &str) -> impl Future<Output = Result<()>> + Send;

    /// Stop listening and close all connections.
    fn stop(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Get number of connected clients.
    fn connected_clients(&self) -> usize;
}

/// Data event for event-driven protocols.
#[derive(Debug, Clone)]
pub enum DataEvent {
    /// Data update received.
    DataUpdate(DataBatch),

    /// Connection state changed.
    ConnectionChanged(ConnectionState),

    /// Error occurred.
    Error(String),

    /// Heartbeat/keep-alive.
    Heartbeat,
}

/// Event receiver type.
pub type DataEventReceiver = mpsc::Receiver<DataEvent>;

/// Event sender type.
pub type DataEventSender = mpsc::Sender<DataEvent>;

/// Event handler trait.
///
/// This trait uses `async_trait` because it needs to be object-safe for `dyn DataEventHandler`.
#[async_trait]
pub trait DataEventHandler: Send + Sync {
    /// Handle data update event.
    async fn on_data_update(&self, batch: DataBatch);

    /// Handle connection state change.
    async fn on_connection_changed(&self, state: ConnectionState);

    /// Handle error event.
    async fn on_error(&self, error: &str);
}

/// Event-driven protocol extension trait.
pub trait EventDrivenProtocol: Protocol {
    /// Subscribe to data events.
    fn subscribe(&self) -> DataEventReceiver;

    /// Set event handler.
    fn set_event_handler(&mut self, handler: Arc<dyn DataEventHandler>);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_state() {
        assert!(!ConnectionState::Disconnected.is_connected());
        assert!(ConnectionState::Connected.is_connected());
        assert!(ConnectionState::Disconnected.can_retry());
        assert!(!ConnectionState::Connecting.can_retry());
    }

    #[test]
    fn test_read_request() {
        let req = ReadRequest::telemetry();
        assert_eq!(req.data_type, Some(DataType::Telemetry));
        assert!(req.point_ids.is_none());
    }

    #[test]
    fn test_control_command() {
        let cmd = ControlCommand::latching(1, true);
        assert!(cmd.pulse_duration_ms.is_none());

        let cmd = ControlCommand::pulse(1, true, 500);
        assert_eq!(cmd.pulse_duration_ms, Some(500));
    }

    #[test]
    fn test_read_response_success() {
        let batch = DataBatch::new();
        let resp = ReadResponse::success(batch);
        assert!(!resp.has_errors());
        assert!(resp.error_summary().is_none());
    }

    #[test]
    fn test_read_response_with_errors() {
        let batch = DataBatch::new();
        let errors = vec![(1, "error 1".to_string()), (2, "error 2".to_string())];
        let resp = ReadResponse::with_errors(batch, errors);

        assert!(resp.has_errors());
        assert_eq!(resp.failed_count, 2);
        assert_eq!(resp.partial_errors.len(), 2);

        let summary = resp.error_summary().unwrap();
        assert_eq!(summary.0, 2); // count
        assert_eq!(summary.1.len(), 2); // first 2 errors
    }

    #[test]
    fn test_read_response_error_summary_limits() {
        let batch = DataBatch::new();
        let errors: Vec<_> = (0..10).map(|i| (i, format!("error {}", i))).collect();
        let resp = ReadResponse::with_errors(batch, errors);

        let summary = resp.error_summary().unwrap();
        assert_eq!(summary.0, 10); // total count
        assert_eq!(summary.1.len(), 3); // only first 3
    }

    #[test]
    fn test_read_response_partial_backward_compat() {
        // partial() sets failed_count but no error details
        let batch = DataBatch::new();
        let resp = ReadResponse::partial(batch, 5);

        assert!(resp.has_errors());
        assert_eq!(resp.failed_count, 5);
        assert!(resp.partial_errors.is_empty());

        // error_summary should still work
        let summary = resp.error_summary().unwrap();
        assert_eq!(summary.0, 5); // count from failed_count
        assert!(summary.1.is_empty()); // no details
    }
}
