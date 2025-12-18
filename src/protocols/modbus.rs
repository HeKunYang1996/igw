//! Modbus protocol adapter.
//!
//! This module provides the `ModbusChannel` adapter that integrates
//! `voltage_modbus` with igw's `Protocol` and `ProtocolClient` traits.
//!
//! # Example
//!
//! ```rust,ignore
//! use igw::prelude::*;
//! use igw::protocols::modbus::{ModbusChannel, ModbusChannelConfig};
//!
//! let config = ModbusChannelConfig::tcp("192.168.1.100:502")
//!     .with_timeout(Duration::from_secs(5));
//!
//! let mut channel = ModbusChannel::new(config, store);
//! channel.connect().await?;
//! ```

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::RwLock;
use voltage_modbus::{ModbusClient, ModbusTcpClient};

use crate::core::data::{DataPoint, DataType, Value};
use crate::core::error::{GatewayError, Result};
use crate::core::point::{DataFormat, PointConfig, ProtocolAddress};
use crate::core::traits::{
    AdjustmentCommand, CommunicationMode, ConnectionState, ControlCommand, Diagnostics,
    PollingConfig, Protocol, ProtocolCapabilities, ProtocolClient, ReadRequest, ReadResponse,
    WriteResult,
};
use crate::store::DataStore;

/// Modbus channel configuration.
#[derive(Debug, Clone)]
pub struct ModbusChannelConfig {
    /// Target address (e.g., "192.168.1.100:502")
    pub address: String,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// I/O operation timeout
    pub io_timeout: Duration,

    /// Point configurations
    pub points: Vec<PointConfig>,
}

impl ModbusChannelConfig {
    /// Create a TCP configuration.
    pub fn tcp(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(3),
            points: Vec::new(),
        }
    }

    /// Set connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set I/O timeout.
    pub fn with_io_timeout(mut self, timeout: Duration) -> Self {
        self.io_timeout = timeout;
        self
    }

    /// Add point configurations.
    pub fn with_points(mut self, points: Vec<PointConfig>) -> Self {
        self.points = points;
        self
    }
}

/// Modbus channel adapter.
///
/// This struct wraps a `voltage_modbus::ModbusTcpClient` and implements
/// igw's `Protocol` and `ProtocolClient` traits.
pub struct ModbusChannel<S: DataStore> {
    config: ModbusChannelConfig,
    client: Option<ModbusTcpClient>,
    store: Arc<S>,
    state: Arc<std::sync::RwLock<ConnectionState>>,
    diagnostics: Arc<RwLock<ChannelDiagnostics>>,
}

#[derive(Debug, Default)]
struct ChannelDiagnostics {
    read_count: u64,
    write_count: u64,
    error_count: u64,
    last_error: Option<String>,
}

impl<S: DataStore> ModbusChannel<S> {
    /// Create a new Modbus channel.
    pub fn new(config: ModbusChannelConfig, store: Arc<S>) -> Self {
        Self {
            config,
            client: None,
            store,
            state: Arc::new(std::sync::RwLock::new(ConnectionState::Disconnected)),
            diagnostics: Arc::new(RwLock::new(ChannelDiagnostics::default())),
        }
    }

    /// Set connection state (internal helper).
    fn set_state(&self, state: ConnectionState) {
        if let Ok(mut s) = self.state.write() {
            *s = state;
        }
    }

    /// Get connection state.
    fn get_state(&self) -> ConnectionState {
        self.state.read().map(|s| *s).unwrap_or(ConnectionState::Error)
    }

    /// Get the point configurations.
    pub fn points(&self) -> &[PointConfig] {
        &self.config.points
    }

    /// Read a single Modbus address and convert to DataPoint.
    async fn read_modbus_point(
        &mut self,
        point: &PointConfig,
    ) -> Result<DataPoint> {
        let client = self.client.as_mut().ok_or(GatewayError::NotConnected)?;

        let modbus_addr = match &point.address {
            ProtocolAddress::Modbus(addr) => addr,
            _ => return Err(GatewayError::Config("Invalid address type".into())),
        };

        // Read registers based on function code
        let value = match modbus_addr.function_code {
            1 => {
                // Read coils (FC01)
                let coils = client
                    .read_01(modbus_addr.slave_id, modbus_addr.register, 1)
                    .await
                    .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                Value::Bool(coils.first().copied().unwrap_or(false))
            }
            2 => {
                // Read discrete inputs (FC02)
                let inputs = client
                    .read_02(modbus_addr.slave_id, modbus_addr.register, 1)
                    .await
                    .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                Value::Bool(inputs.first().copied().unwrap_or(false))
            }
            3 => {
                // Read holding registers (FC03)
                let count = modbus_addr.format.register_count();
                let regs = client
                    .read_03(modbus_addr.slave_id, modbus_addr.register, count)
                    .await
                    .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                decode_registers(&regs, modbus_addr.format, modbus_addr.byte_order, modbus_addr.bit_position)?
            }
            4 => {
                // Read input registers (FC04)
                let count = modbus_addr.format.register_count();
                let regs = client
                    .read_04(modbus_addr.slave_id, modbus_addr.register, count)
                    .await
                    .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                decode_registers(&regs, modbus_addr.format, modbus_addr.byte_order, modbus_addr.bit_position)?
            }
            _ => return Err(GatewayError::Unsupported(format!(
                "Function code {} not supported for read",
                modbus_addr.function_code
            ))),
        };

        // Apply transform
        let transformed_value = apply_transform(value, &point.transform);

        Ok(DataPoint::new(&point.id, point.data_type, transformed_value))
    }

    /// Record an error in diagnostics.
    async fn record_error(&self, error: &str) {
        let mut diag = self.diagnostics.write().await;
        diag.error_count += 1;
        diag.last_error = Some(error.to_string());
    }
}

impl<S: DataStore> ProtocolCapabilities for ModbusChannel<S> {
    fn name(&self) -> &'static str {
        "Modbus TCP"
    }

    fn supported_modes(&self) -> &[CommunicationMode] {
        &[CommunicationMode::Polling]
    }

    fn version(&self) -> &'static str {
        "1.0"
    }
}

#[async_trait]
impl<S: DataStore + 'static> Protocol for ModbusChannel<S> {
    fn connection_state(&self) -> ConnectionState {
        self.get_state()
    }

    async fn read(&self, _request: ReadRequest) -> Result<ReadResponse> {
        // Note: This requires &mut self, but trait uses &self
        // For now, return error - use ProtocolClient::read instead
        Err(GatewayError::Internal(
            "Use ProtocolClient methods for Modbus reads".into(),
        ))
    }

    async fn diagnostics(&self) -> Result<Diagnostics> {
        let state = self.get_state();
        let diag = self.diagnostics.read().await;

        Ok(Diagnostics {
            protocol: self.name().to_string(),
            connection_state: state,
            read_count: diag.read_count,
            write_count: diag.write_count,
            error_count: diag.error_count,
            last_error: diag.last_error.clone(),
            extra: serde_json::json!({
                "address": self.config.address,
                "points": self.config.points.len(),
            }),
        })
    }
}

#[async_trait]
impl<S: DataStore + 'static> ProtocolClient for ModbusChannel<S> {
    async fn connect(&mut self) -> Result<()> {
        self.set_state(ConnectionState::Connecting);

        match ModbusTcpClient::from_address(&self.config.address, self.config.connect_timeout).await
        {
            Ok(client) => {
                self.client = Some(client);
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
        if let Some(mut client) = self.client.take() {
            let _ = client.close().await;
        }
        self.set_state(ConnectionState::Disconnected);
        Ok(())
    }

    async fn write_control(&mut self, commands: &[ControlCommand]) -> Result<WriteResult> {
        let mut success_count = 0;
        let mut failures = Vec::new();
        let mut errors_to_record = Vec::new();

        for cmd in commands {
            // Find point config
            let point = self
                .config
                .points
                .iter()
                .find(|p| p.id == cmd.id && p.data_type == DataType::Control);

            let point = match point {
                Some(p) => p,
                None => {
                    failures.push((cmd.id.clone(), "Point not found".into()));
                    continue;
                }
            };

            let modbus_addr = match &point.address {
                ProtocolAddress::Modbus(addr) => addr.clone(),
                _ => {
                    failures.push((cmd.id.clone(), "Invalid address type".into()));
                    continue;
                }
            };

            // Get client reference
            let client = match self.client.as_mut() {
                Some(c) => c,
                None => return Err(GatewayError::NotConnected),
            };

            // Write coil (FC05)
            let result = client
                .write_05(modbus_addr.slave_id, modbus_addr.register, cmd.value)
                .await;

            match result {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    failures.push((cmd.id.clone(), err_msg.clone()));
                    errors_to_record.push(err_msg);
                }
            }
        }

        // Record errors and update diagnostics after loop
        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += success_count as u64;
            if let Some(err) = errors_to_record.last() {
                diag.error_count += errors_to_record.len() as u64;
                diag.last_error = Some(err.clone());
            }
        }

        Ok(WriteResult {
            success_count,
            failures,
        })
    }

    async fn write_adjustment(&mut self, adjustments: &[AdjustmentCommand]) -> Result<WriteResult> {
        let mut success_count = 0;
        let mut failures = Vec::new();
        let mut errors_to_record = Vec::new();

        for adj in adjustments {
            // Find point config
            let point = self
                .config
                .points
                .iter()
                .find(|p| p.id == adj.id && p.data_type == DataType::Adjustment);

            let point = match point {
                Some(p) => p,
                None => {
                    failures.push((adj.id.clone(), "Point not found".into()));
                    continue;
                }
            };

            let modbus_addr = match &point.address {
                ProtocolAddress::Modbus(addr) => addr.clone(),
                _ => {
                    failures.push((adj.id.clone(), "Invalid address type".into()));
                    continue;
                }
            };

            // Apply reverse transform to get raw value
            let raw_value = reverse_transform(adj.value, &point.transform);

            // Get client reference
            let client = match self.client.as_mut() {
                Some(c) => c,
                None => return Err(GatewayError::NotConnected),
            };

            // Encode and write register(s)
            let result = match modbus_addr.format {
                DataFormat::UInt16 | DataFormat::Int16 => {
                    let value = raw_value as u16;
                    client
                        .write_06(modbus_addr.slave_id, modbus_addr.register, value)
                        .await
                }
                DataFormat::UInt32 | DataFormat::Int32 | DataFormat::Float32 => {
                    let regs = encode_value(raw_value, modbus_addr.format, modbus_addr.byte_order)?;
                    client
                        .write_10(modbus_addr.slave_id, modbus_addr.register, &regs)
                        .await
                }
                _ => {
                    failures.push((adj.id.clone(), "Unsupported format for write".into()));
                    continue;
                }
            };

            match result {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    failures.push((adj.id.clone(), err_msg.clone()));
                    errors_to_record.push(err_msg);
                }
            }
        }

        // Record errors and update diagnostics after loop
        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += success_count as u64;
            if let Some(err) = errors_to_record.last() {
                diag.error_count += errors_to_record.len() as u64;
                diag.last_error = Some(err.clone());
            }
        }

        Ok(WriteResult {
            success_count,
            failures,
        })
    }

    async fn start_polling(&mut self, _config: PollingConfig) -> Result<()> {
        // TODO: Implement polling task
        Ok(())
    }

    async fn stop_polling(&mut self) -> Result<()> {
        // TODO: Implement stop polling
        Ok(())
    }
}

/// Decode Modbus registers to a Value.
fn decode_registers(
    regs: &[u16],
    format: crate::core::point::DataFormat,
    byte_order: crate::core::point::ByteOrder,
    bit_position: Option<u8>,
) -> Result<Value> {
    use crate::codec::byte_order::decode_registers as codec_decode;

    codec_decode(regs, format, byte_order, bit_position)
}

/// Encode a Value to Modbus registers.
fn encode_value(
    value: f64,
    format: crate::core::point::DataFormat,
    byte_order: crate::core::point::ByteOrder,
) -> Result<Vec<u16>> {
    use crate::codec::byte_order::encode_registers;

    encode_registers(&Value::Float(value), format, byte_order)
}

/// Apply transform to a value.
fn apply_transform(value: Value, transform: &crate::core::point::TransformConfig) -> Value {
    match value {
        Value::Float(v) => Value::Float(transform.apply(v)),
        Value::Integer(v) => Value::Float(transform.apply(v as f64)),
        Value::Bool(v) => Value::Bool(transform.apply_bool(v)),
        other => other,
    }
}

/// Reverse transform to get raw value.
fn reverse_transform(value: f64, transform: &crate::core::point::TransformConfig) -> f64 {
    transform.reverse_apply(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::MemoryStore;

    #[test]
    fn test_modbus_channel_config() {
        let config = ModbusChannelConfig::tcp("127.0.0.1:502")
            .with_connect_timeout(Duration::from_secs(10))
            .with_io_timeout(Duration::from_secs(5));

        assert_eq!(config.address, "127.0.0.1:502");
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.io_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_modbus_channel_capabilities() {
        let store = Arc::new(MemoryStore::new());
        let config = ModbusChannelConfig::tcp("127.0.0.1:502");
        let channel = ModbusChannel::new(config, store);

        assert_eq!(channel.name(), "Modbus TCP");
        assert_eq!(channel.supported_modes(), &[CommunicationMode::Polling]);
    }
}
