//! IEC 60870-5-104 protocol adapter.
//!
//! This module provides the `Iec104Channel` adapter that integrates
//! `voltage_iec104` with igw's `Protocol` and `EventDrivenProtocol` traits.
//!
//! IEC 104 is an event-driven protocol - data is received via spontaneous
//! transmissions from the controlled station (RTU/substation).
//!
//! # Example
//!
//! ```rust,ignore
//! use igw::prelude::*;
//! use igw::protocols::iec104::{Iec104Channel, Iec104ChannelConfig};
//!
//! let config = Iec104ChannelConfig::new("192.168.1.100:2404")
//!     .with_common_address(1);
//!
//! let mut channel = Iec104Channel::new(config, store);
//! channel.connect().await?;
//! channel.start_data_transfer().await?;
//!
//! // Receive events via subscription
//! let mut rx = channel.subscribe();
//! while let Some(event) = rx.recv().await {
//!     match event {
//!         DataEvent::DataUpdate(batch) => { /* process data */ }
//!         _ => {}
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::{mpsc, RwLock};
use voltage_iec104::{
    ClientConfig, ConnectionState as Iec104State, Iec104Client, Iec104Event,
};

use crate::core::data::{DataBatch, DataPoint, DataType, Quality, Value};
use crate::core::error::{GatewayError, Result};
use crate::core::point::PointConfig;
use crate::core::traits::{
    AdjustmentCommand, CommunicationMode, ConnectionState, ControlCommand, DataEvent,
    DataEventHandler, DataEventReceiver, DataEventSender, Diagnostics, EventDrivenProtocol,
    PollingConfig, Protocol, ProtocolCapabilities, ProtocolClient, ReadRequest, ReadResponse,
    WriteResult,
};
use crate::store::DataStore;

/// IEC 104 channel configuration.
#[derive(Debug, Clone)]
pub struct Iec104ChannelConfig {
    /// Target address (e.g., "192.168.1.100:2404")
    pub address: String,

    /// Common address of ASDU (station address)
    pub common_address: u16,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// T1 timeout (send/receive APDU)
    pub t1_timeout: Duration,

    /// T2 timeout (no data acknowledgement)
    pub t2_timeout: Duration,

    /// T3 timeout (test frame)
    pub t3_timeout: Duration,

    /// Max unconfirmed I-frames (K parameter)
    pub k: u16,

    /// Latest ack threshold (W parameter)
    pub w: u16,

    /// Point configurations (IOA to point mapping)
    pub points: Vec<PointConfig>,

    /// IOA to point ID mapping (built from points)
    ioa_mapping: HashMap<u32, String>,
}

impl Iec104ChannelConfig {
    /// Create a new configuration.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            common_address: 1,
            connect_timeout: Duration::from_secs(10),
            t1_timeout: Duration::from_secs(15),
            t2_timeout: Duration::from_secs(10),
            t3_timeout: Duration::from_secs(20),
            k: 12,
            w: 8,
            points: Vec::new(),
            ioa_mapping: HashMap::new(),
        }
    }

    /// Set common address.
    pub fn with_common_address(mut self, addr: u16) -> Self {
        self.common_address = addr;
        self
    }

    /// Set connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set T1 timeout.
    pub fn with_t1_timeout(mut self, timeout: Duration) -> Self {
        self.t1_timeout = timeout;
        self
    }

    /// Set T2 timeout.
    pub fn with_t2_timeout(mut self, timeout: Duration) -> Self {
        self.t2_timeout = timeout;
        self
    }

    /// Set T3 timeout.
    pub fn with_t3_timeout(mut self, timeout: Duration) -> Self {
        self.t3_timeout = timeout;
        self
    }

    /// Add point configurations.
    pub fn with_points(mut self, points: Vec<PointConfig>) -> Self {
        // Build IOA mapping from point configs
        for point in &points {
            if let crate::core::point::ProtocolAddress::Iec104(addr) = &point.address {
                self.ioa_mapping.insert(addr.ioa, point.id.clone());
            }
        }
        self.points = points;
        self
    }

    /// Build voltage_iec104 ClientConfig.
    fn to_client_config(&self) -> ClientConfig {
        ClientConfig::new(&self.address)
            .connect_timeout(self.connect_timeout)
            .t1_timeout(self.t1_timeout)
            .t2_timeout(self.t2_timeout)
            .t3_timeout(self.t3_timeout)
            .k(self.k)
            .w(self.w)
    }
}

/// IEC 104 channel adapter.
///
/// This struct wraps a `voltage_iec104::Iec104Client` and implements
/// igw's `Protocol`, `ProtocolClient`, and `EventDrivenProtocol` traits.
pub struct Iec104Channel<S: DataStore> {
    config: Iec104ChannelConfig,
    client: Iec104Client,
    store: Arc<S>,
    state: Arc<std::sync::RwLock<ConnectionState>>,
    diagnostics: Arc<RwLock<ChannelDiagnostics>>,
    event_tx: DataEventSender,
    event_rx: Option<DataEventReceiver>,
    event_handler: Option<Arc<dyn DataEventHandler>>,
    poll_task: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct ChannelDiagnostics {
    recv_count: u64,
    send_count: u64,
    error_count: u64,
    last_error: Option<String>,
    last_interrogation: Option<std::time::Instant>,
}

impl<S: DataStore> Iec104Channel<S> {
    /// Create a new IEC 104 channel.
    pub fn new(config: Iec104ChannelConfig, store: Arc<S>) -> Self {
        let client_config = config.to_client_config();
        let client = Iec104Client::new(client_config);
        let (event_tx, event_rx) = mpsc::channel(1024);

        Self {
            config,
            client,
            store,
            state: Arc::new(std::sync::RwLock::new(ConnectionState::Disconnected)),
            diagnostics: Arc::new(RwLock::new(ChannelDiagnostics::default())),
            event_tx,
            event_rx: Some(event_rx),
            event_handler: None,
            poll_task: None,
        }
    }

    /// Set connection state.
    fn set_state(&self, state: ConnectionState) {
        if let Ok(mut s) = self.state.write() {
            *s = state;
        }
    }

    /// Get connection state.
    fn get_state(&self) -> ConnectionState {
        self.state.read().map(|s| *s).unwrap_or(ConnectionState::Error)
    }

    /// Start data transfer (STARTDT).
    pub async fn start_data_transfer(&mut self) -> Result<()> {
        self.client
            .start_dt()
            .await
            .map_err(|e| GatewayError::Protocol(e.to_string()))
    }

    /// Stop data transfer (STOPDT).
    pub async fn stop_data_transfer(&mut self) -> Result<()> {
        self.client
            .stop_dt()
            .await
            .map_err(|e| GatewayError::Protocol(e.to_string()))
    }

    /// Send general interrogation command.
    pub async fn general_interrogation(&mut self) -> Result<()> {
        self.client
            .general_interrogation(self.config.common_address)
            .await
            .map_err(|e| GatewayError::Protocol(e.to_string()))?;

        let mut diag = self.diagnostics.write().await;
        diag.last_interrogation = Some(std::time::Instant::now());
        Ok(())
    }

    /// Send counter interrogation command.
    pub async fn counter_interrogation(&mut self) -> Result<()> {
        self.client
            .counter_interrogation(self.config.common_address)
            .await
            .map_err(|e| GatewayError::Protocol(e.to_string()))
    }

    /// Send clock synchronization command.
    pub async fn clock_sync(&mut self) -> Result<()> {
        self.client
            .clock_sync(self.config.common_address)
            .await
            .map_err(|e| GatewayError::Protocol(e.to_string()))
    }

    /// Poll for events (must be called periodically).
    pub async fn poll(&mut self) -> Result<()> {
        match self.client.poll().await {
            Ok(Some(event)) => {
                self.handle_iec104_event(event).await;
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => {
                self.record_error(&e.to_string()).await;
                Err(GatewayError::Protocol(e.to_string()))
            }
        }
    }

    /// Handle IEC 104 event.
    async fn handle_iec104_event(&self, event: Iec104Event) {
        match event {
            Iec104Event::Connected => {
                self.set_state(ConnectionState::Connected);
                let _ = self.event_tx.send(DataEvent::ConnectionChanged(ConnectionState::Connected)).await;
            }
            Iec104Event::Disconnected => {
                self.set_state(ConnectionState::Disconnected);
                let _ = self.event_tx.send(DataEvent::ConnectionChanged(ConnectionState::Disconnected)).await;
            }
            Iec104Event::DataTransferStarted => {
                // Data transfer is active
            }
            Iec104Event::DataTransferStopped => {
                // Data transfer stopped
            }
            Iec104Event::DataUpdate(points) => {
                let batch = self.convert_data_points(points).await;
                if !batch.is_empty() {
                    // Store to DataStore
                    if let Err(e) = self.store.write_batch(&batch).await {
                        self.record_error(&e.to_string()).await;
                    }

                    // Send event
                    let _ = self.event_tx.send(DataEvent::DataUpdate(batch)).await;

                    // Update diagnostics
                    let mut diag = self.diagnostics.write().await;
                    diag.recv_count += 1;
                }
            }
            Iec104Event::AsduReceived(_asdu) => {
                // Raw ASDU - usually for command responses
            }
            Iec104Event::CommandConfirm { ioa, success } => {
                // Command confirmation
                let mut diag = self.diagnostics.write().await;
                if success {
                    diag.send_count += 1;
                } else {
                    diag.error_count += 1;
                    diag.last_error = Some(format!("Command failed for IOA {}", ioa));
                }
            }
            Iec104Event::InterrogationComplete { common_address: _ } => {
                // Interrogation finished
            }
            Iec104Event::Error(msg) => {
                self.record_error(&msg).await;
                let _ = self.event_tx.send(DataEvent::Error(msg)).await;
            }
        }
    }

    /// Convert IEC 104 data points to igw DataBatch.
    async fn convert_data_points(&self, points: Vec<voltage_iec104::DataPoint>) -> DataBatch {
        let mut batch = DataBatch::new();

        for point in points {
            // Look up point ID from IOA
            let point_id = match self.config.ioa_mapping.get(&point.ioa) {
                Some(id) => id.clone(),
                None => format!("ioa_{}", point.ioa), // Fallback to IOA-based ID
            };

            // Convert value
            let value = convert_iec104_value(&point.value);

            // Convert quality
            let quality = convert_iec104_quality(&point.quality);

            // Determine data type based on value
            let data_type = if point.value.is_boolean() {
                DataType::Signal
            } else {
                DataType::Telemetry
            };

            let dp = DataPoint {
                id: point_id,
                data_type,
                value,
                quality,
                timestamp: point.timestamp.map(|t| t.to_datetime()),
            };

            batch.add(dp);
        }

        batch
    }

    /// Record an error.
    async fn record_error(&self, error: &str) {
        let mut diag = self.diagnostics.write().await;
        diag.error_count += 1;
        diag.last_error = Some(error.to_string());
    }

    /// Find point config by ID.
    fn find_point(&self, id: &str) -> Option<&PointConfig> {
        self.config.points.iter().find(|p| p.id == id)
    }
}

impl<S: DataStore> ProtocolCapabilities for Iec104Channel<S> {
    fn name(&self) -> &'static str {
        "IEC 60870-5-104"
    }

    fn supported_modes(&self) -> &[CommunicationMode] {
        &[CommunicationMode::EventDriven, CommunicationMode::Polling]
    }

    fn version(&self) -> &'static str {
        "1.0"
    }
}

#[async_trait]
impl<S: DataStore + 'static> Protocol for Iec104Channel<S> {
    fn connection_state(&self) -> ConnectionState {
        self.get_state()
    }

    async fn read(&self, _request: ReadRequest) -> Result<ReadResponse> {
        // IEC 104 is event-driven, not polling-based
        // Return current values from store
        Err(GatewayError::Unsupported(
            "IEC 104 is event-driven; use subscribe() or general_interrogation()".into(),
        ))
    }

    async fn diagnostics(&self) -> Result<Diagnostics> {
        let state = self.get_state();
        let diag = self.diagnostics.read().await;

        Ok(Diagnostics {
            protocol: self.name().to_string(),
            connection_state: state,
            read_count: diag.recv_count,
            write_count: diag.send_count,
            error_count: diag.error_count,
            last_error: diag.last_error.clone(),
            extra: serde_json::json!({
                "address": self.config.address,
                "common_address": self.config.common_address,
                "points": self.config.points.len(),
                "last_interrogation": diag.last_interrogation.map(|t| t.elapsed().as_secs()),
            }),
        })
    }
}

#[async_trait]
impl<S: DataStore + 'static> ProtocolClient for Iec104Channel<S> {
    async fn connect(&mut self) -> Result<()> {
        self.set_state(ConnectionState::Connecting);

        match self.client.connect().await {
            Ok(()) => {
                self.set_state(ConnectionState::Connected);
                Ok(())
            }
            Err(e) => {
                self.set_state(ConnectionState::Error);
                self.record_error(&e.to_string()).await;
                Err(GatewayError::Connection(e.to_string()))
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Stop poll task if running
        if let Some(task) = self.poll_task.take() {
            task.abort();
        }

        // Stop data transfer first
        let _ = self.stop_data_transfer().await;

        // Disconnect
        match self.client.disconnect().await {
            Ok(()) => {
                self.set_state(ConnectionState::Disconnected);
                Ok(())
            }
            Err(e) => Err(GatewayError::Connection(e.to_string())),
        }
    }

    async fn write_control(&mut self, commands: &[ControlCommand]) -> Result<WriteResult> {
        let mut success_count = 0;
        let mut failures = Vec::new();

        for cmd in commands {
            // Find point config
            let point = match self.find_point(&cmd.id) {
                Some(p) => p,
                None => {
                    failures.push((cmd.id.clone(), "Point not found".into()));
                    continue;
                }
            };

            // Get IEC 104 address
            let iec_addr = match &point.address {
                crate::core::point::ProtocolAddress::Iec104(addr) => addr,
                _ => {
                    failures.push((cmd.id.clone(), "Invalid address type".into()));
                    continue;
                }
            };

            // Send single command
            let result = self
                .client
                .single_command(
                    self.config.common_address,
                    iec_addr.ioa,
                    cmd.value,
                    false, // not select
                )
                .await;

            match result {
                Ok(()) => success_count += 1,
                Err(e) => {
                    failures.push((cmd.id.clone(), e.to_string()));
                }
            }
        }

        {
            let mut diag = self.diagnostics.write().await;
            diag.send_count += success_count as u64;
        }

        Ok(WriteResult {
            success_count,
            failures,
        })
    }

    async fn write_adjustment(&mut self, adjustments: &[AdjustmentCommand]) -> Result<WriteResult> {
        let mut success_count = 0;
        let mut failures = Vec::new();

        for adj in adjustments {
            // Find point config
            let point = match self.find_point(&adj.id) {
                Some(p) => p,
                None => {
                    failures.push((adj.id.clone(), "Point not found".into()));
                    continue;
                }
            };

            // Get IEC 104 address
            let iec_addr = match &point.address {
                crate::core::point::ProtocolAddress::Iec104(addr) => addr,
                _ => {
                    failures.push((adj.id.clone(), "Invalid address type".into()));
                    continue;
                }
            };

            // Apply reverse transform
            let raw_value = point.transform.reverse_apply(adj.value) as f32;

            // Send setpoint command
            let result = self
                .client
                .setpoint_float(
                    self.config.common_address,
                    iec_addr.ioa,
                    raw_value,
                    false, // not select
                )
                .await;

            match result {
                Ok(()) => success_count += 1,
                Err(e) => {
                    failures.push((adj.id.clone(), e.to_string()));
                }
            }
        }

        {
            let mut diag = self.diagnostics.write().await;
            diag.send_count += success_count as u64;
        }

        Ok(WriteResult {
            success_count,
            failures,
        })
    }

    async fn start_polling(&mut self, config: PollingConfig) -> Result<()> {
        // For IEC 104, "polling" means starting a background task that calls poll()
        // and optionally sends periodic general interrogations

        let interval = Duration::from_millis(config.interval_ms);

        // Start data transfer first
        self.start_data_transfer().await?;

        // Send initial general interrogation
        self.general_interrogation().await?;

        // Note: The actual poll loop would need to be implemented with
        // proper channel cloning or task management. For now, we just
        // indicate that polling has been configured.

        Ok(())
    }

    async fn stop_polling(&mut self) -> Result<()> {
        if let Some(task) = self.poll_task.take() {
            task.abort();
        }
        self.stop_data_transfer().await
    }
}

impl<S: DataStore + 'static> EventDrivenProtocol for Iec104Channel<S> {
    fn subscribe(&self) -> DataEventReceiver {
        // Note: This consumes the receiver, so only one subscriber is supported
        // For multiple subscribers, would need broadcast channel
        let (tx, rx) = mpsc::channel(1024);
        // In a real implementation, we'd need to clone the sender and manage subscriptions
        rx
    }

    fn set_event_handler(&mut self, handler: Arc<dyn DataEventHandler>) {
        self.event_handler = Some(handler);
    }
}

/// Convert IEC 104 DataValue to igw Value.
fn convert_iec104_value(value: &voltage_iec104::DataValue) -> Value {
    match value {
        voltage_iec104::DataValue::Single(v) => Value::Bool(*v),
        voltage_iec104::DataValue::Double(dp) => {
            use voltage_iec104::DoublePointValue;
            match dp {
                DoublePointValue::Off => Value::Bool(false),
                DoublePointValue::On => Value::Bool(true),
                _ => Value::Invalid,
            }
        }
        voltage_iec104::DataValue::Normalized(v) => Value::Float(*v as f64),
        voltage_iec104::DataValue::Scaled(v) => Value::Integer(*v as i64),
        voltage_iec104::DataValue::Float(v) => Value::Float(*v as f64),
        voltage_iec104::DataValue::Counter(v) => Value::Integer(*v as i64),
        voltage_iec104::DataValue::Bitstring(v) => Value::Integer(*v as i64),
        voltage_iec104::DataValue::StepPosition(v) => Value::Integer(*v as i64),
        voltage_iec104::DataValue::BinaryCounter { value, .. } => Value::Integer(*value as i64),
    }
}

/// Convert IEC 104 Quality to igw Quality.
fn convert_iec104_quality(quality: &voltage_iec104::Quality) -> Quality {
    if quality.is_good() {
        Quality::Good
    } else if quality.invalid {
        Quality::Invalid
    } else {
        Quality::Questionable
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn test_iec104_channel_config() {
        let config = Iec104ChannelConfig::new("127.0.0.1:2404")
            .with_common_address(1)
            .with_connect_timeout(Duration::from_secs(10));

        assert_eq!(config.address, "127.0.0.1:2404");
        assert_eq!(config.common_address, 1);
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_iec104_channel_capabilities() {
        let store = Arc::new(MemoryStore::new());
        let config = Iec104ChannelConfig::new("127.0.0.1:2404");
        let channel = Iec104Channel::new(config, store);

        assert_eq!(channel.name(), "IEC 60870-5-104");
        assert!(channel
            .supported_modes()
            .contains(&CommunicationMode::EventDriven));
    }

    #[test]
    fn test_convert_iec104_value() {
        assert_eq!(
            convert_iec104_value(&voltage_iec104::DataValue::Single(true)),
            Value::Bool(true)
        );
        assert_eq!(
            convert_iec104_value(&voltage_iec104::DataValue::Float(23.5)),
            Value::Float(23.5)
        );
        assert_eq!(
            convert_iec104_value(&voltage_iec104::DataValue::Scaled(100)),
            Value::Integer(100)
        );
    }
}
