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
//! // Create channel without DataStore dependency
//! let config = ModbusChannelConfig::tcp("192.168.1.100:502")
//!     .with_connect_timeout(Duration::from_secs(5));
//!
//! let mut channel = ModbusChannel::new(config);
//! channel.connect().await?;
//!
//! // Service layer polls and handles storage
//! let batch = channel.poll_once().await?;
//! store.write_batch(channel_id, &batch).await?;
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::interval;
use tracing::{debug, info, warn};
use voltage_modbus::{ModbusClient, ModbusTcpClient};

#[cfg(feature = "modbus")]
use voltage_modbus::ModbusRtuClient;

use crate::core::data::{DataBatch, DataPoint, DataType, Value};
use crate::core::error::{GatewayError, Result};
use crate::core::logging::{
    ChannelLogConfig, ChannelLogHandler, ErrorContext, LogContext, LoggableProtocol,
};
use crate::core::metadata::{DriverMetadata, HasMetadata, ParameterMetadata, ParameterType};
use crate::core::point::{DataFormat, PointConfig, ProtocolAddress};
use crate::core::traits::{
    AdjustmentCommand, CommunicationMode, ConnectionState, ControlCommand, Diagnostics,
    PollingConfig, Protocol, ProtocolCapabilities, ProtocolClient, ReadRequest, ReadResponse,
    WriteResult,
};
use crate::protocols::command_batcher::{BatchCommand, CommandBatcher};

// Type alias for grouped points: (slave_id, function_code) -> Vec<PointConfig>
type GroupedPoints = HashMap<(u8, u8), Vec<PointConfig>>;

/// Default maximum registers per batch read
const DEFAULT_MAX_BATCH_SIZE: u16 = 125;

/// Default maximum gap between registers to allow merging
const DEFAULT_MAX_GAP: u16 = 10;

/// Default reconnect cooldown in milliseconds (60 seconds)
const DEFAULT_RECONNECT_COOLDOWN_MS: u64 = 60_000;

/// Default maximum reconnect attempts (0 = unlimited)
const DEFAULT_MAX_RECONNECT_ATTEMPTS: u32 = 0;

/// Default consecutive zero-data cycles before triggering reconnect
const DEFAULT_ZERO_DATA_THRESHOLD: u32 = 5;

/// Reconnect configuration for automatic connection recovery.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Cooldown period after disconnect before reconnect attempts (in ms)
    pub cooldown_ms: u64,
    /// Maximum reconnect attempts (0 = unlimited)
    pub max_attempts: u32,
    /// Consecutive zero-data polling cycles before triggering reconnect
    pub zero_data_threshold: u32,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            cooldown_ms: DEFAULT_RECONNECT_COOLDOWN_MS,
            max_attempts: DEFAULT_MAX_RECONNECT_ATTEMPTS,
            zero_data_threshold: DEFAULT_ZERO_DATA_THRESHOLD,
        }
    }
}

impl ReconnectConfig {
    /// Create a new reconnect configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set cooldown period.
    pub fn with_cooldown_ms(mut self, ms: u64) -> Self {
        self.cooldown_ms = ms;
        self
    }

    /// Set maximum reconnect attempts.
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Set zero-data threshold.
    pub fn with_zero_data_threshold(mut self, threshold: u32) -> Self {
        self.zero_data_threshold = threshold;
        self
    }
}

/// Internal state for tracking reconnection attempts.
#[derive(Debug, Default)]
struct ReconnectState {
    /// Current reconnect attempt count
    attempts: u32,
    /// Last reconnect attempt timestamp (milliseconds since epoch)
    last_attempt_ms: u64,
    /// Consecutive polling cycles with zero data
    consecutive_zero_cycles: u32,
}

/// Connection mode for Modbus channel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConnectionMode {
    /// TCP/IP connection (default)
    #[default]
    Tcp,
    /// RTU serial port connection
    #[cfg(feature = "modbus")]
    Rtu,
}

/// Unified Modbus client wrapper for TCP and RTU transports.
///
/// This enum provides a transport-agnostic interface for Modbus operations,
/// allowing the same application code to work with both TCP and RTU connections.
pub enum ModbusClientWrapper {
    /// TCP client
    Tcp(ModbusTcpClient),
    /// RTU client (requires `modbus-rtu` feature)
    #[cfg(feature = "modbus")]
    Rtu(ModbusRtuClient),
}

impl ModbusClientWrapper {
    /// Read coils (FC01)
    pub async fn read_01(
        &mut self,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> voltage_modbus::ModbusResult<Vec<bool>> {
        match self {
            Self::Tcp(client) => client.read_01(slave_id, address, quantity).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.read_01(slave_id, address, quantity).await,
        }
    }

    /// Read discrete inputs (FC02)
    pub async fn read_02(
        &mut self,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> voltage_modbus::ModbusResult<Vec<bool>> {
        match self {
            Self::Tcp(client) => client.read_02(slave_id, address, quantity).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.read_02(slave_id, address, quantity).await,
        }
    }

    /// Read holding registers (FC03)
    pub async fn read_03(
        &mut self,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> voltage_modbus::ModbusResult<Vec<u16>> {
        match self {
            Self::Tcp(client) => client.read_03(slave_id, address, quantity).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.read_03(slave_id, address, quantity).await,
        }
    }

    /// Read input registers (FC04)
    pub async fn read_04(
        &mut self,
        slave_id: u8,
        address: u16,
        quantity: u16,
    ) -> voltage_modbus::ModbusResult<Vec<u16>> {
        match self {
            Self::Tcp(client) => client.read_04(slave_id, address, quantity).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.read_04(slave_id, address, quantity).await,
        }
    }

    /// Write single coil (FC05)
    pub async fn write_05(
        &mut self,
        slave_id: u8,
        address: u16,
        value: bool,
    ) -> voltage_modbus::ModbusResult<()> {
        match self {
            Self::Tcp(client) => client.write_05(slave_id, address, value).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.write_05(slave_id, address, value).await,
        }
    }

    /// Write single register (FC06)
    pub async fn write_06(
        &mut self,
        slave_id: u8,
        address: u16,
        value: u16,
    ) -> voltage_modbus::ModbusResult<()> {
        match self {
            Self::Tcp(client) => client.write_06(slave_id, address, value).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.write_06(slave_id, address, value).await,
        }
    }

    /// Write multiple coils (FC0F)
    pub async fn write_0f(
        &mut self,
        slave_id: u8,
        address: u16,
        values: &[bool],
    ) -> voltage_modbus::ModbusResult<()> {
        match self {
            Self::Tcp(client) => client.write_0f(slave_id, address, values).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.write_0f(slave_id, address, values).await,
        }
    }

    /// Write multiple registers (FC10)
    pub async fn write_10(
        &mut self,
        slave_id: u8,
        address: u16,
        values: &[u16],
    ) -> voltage_modbus::ModbusResult<()> {
        match self {
            Self::Tcp(client) => client.write_10(slave_id, address, values).await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.write_10(slave_id, address, values).await,
        }
    }

    /// Close the connection
    pub async fn close(&mut self) -> voltage_modbus::ModbusResult<()> {
        match self {
            Self::Tcp(client) => client.close().await,
            #[cfg(feature = "modbus")]
            Self::Rtu(client) => client.close().await,
        }
    }
}

/// Modbus channel configuration.
#[derive(Debug, Clone)]
pub struct ModbusChannelConfig {
    /// Connection mode (TCP or RTU)
    pub connection_mode: ConnectionMode,

    /// Target address for TCP (e.g., "192.168.1.100:502")
    pub address: String,

    /// Connection timeout (TCP only)
    pub connect_timeout: Duration,

    /// I/O operation timeout
    pub io_timeout: Duration,

    /// RTU serial device path (e.g., "/dev/ttyUSB0")
    #[cfg(feature = "modbus")]
    pub rtu_device: String,

    /// RTU baud rate (e.g., 9600, 19200, 115200)
    #[cfg(feature = "modbus")]
    pub baud_rate: u32,

    /// Point configurations
    pub points: Vec<PointConfig>,

    /// Maximum registers per batch read (default: 125)
    pub max_batch_size: u16,

    /// Maximum gap between registers to allow merging (default: 10)
    pub max_gap: u16,

    /// Reconnect configuration
    pub reconnect: ReconnectConfig,
}

impl ModbusChannelConfig {
    /// Create a TCP configuration.
    pub fn tcp(address: impl Into<String>) -> Self {
        Self {
            connection_mode: ConnectionMode::Tcp,
            address: address.into(),
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(3),
            #[cfg(feature = "modbus")]
            rtu_device: String::new(),
            #[cfg(feature = "modbus")]
            baud_rate: 9600,
            points: Vec::new(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_gap: DEFAULT_MAX_GAP,
            reconnect: ReconnectConfig::default(),
        }
    }

    /// Create an RTU (serial) configuration.
    ///
    /// # Arguments
    ///
    /// * `device` - Serial device path (e.g., "/dev/ttyUSB0", "COM1")
    /// * `baud_rate` - Baud rate (e.g., 9600, 19200, 115200)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ModbusChannelConfig::rtu("/dev/ttyUSB0", 9600);
    /// ```
    #[cfg(feature = "modbus")]
    pub fn rtu(device: impl Into<String>, baud_rate: u32) -> Self {
        Self {
            connection_mode: ConnectionMode::Rtu,
            address: String::new(), // Not used for RTU
            connect_timeout: Duration::from_secs(5),
            io_timeout: Duration::from_secs(3),
            rtu_device: device.into(),
            baud_rate,
            points: Vec::new(),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_gap: DEFAULT_MAX_GAP,
            reconnect: ReconnectConfig::default(),
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

    /// Set maximum batch size for register reads.
    pub fn with_max_batch_size(mut self, size: u16) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Set maximum gap for merging consecutive registers.
    pub fn with_max_gap(mut self, gap: u16) -> Self {
        self.max_gap = gap;
        self
    }

    /// Set reconnect configuration.
    pub fn with_reconnect(mut self, config: ReconnectConfig) -> Self {
        self.reconnect = config;
        self
    }
}

/// Modbus channel adapter.
///
/// This struct wraps a `voltage_modbus::ModbusTcpClient` and implements
/// igw's `Protocol` and `ProtocolClient` traits.
///
/// # Architecture (Protocol Layer Only)
///
/// `ModbusChannel` is a pure protocol implementation that handles device communication.
/// It does NOT handle data storage - that responsibility belongs to the service layer.
///
/// Use `poll_once()` to perform a single poll cycle and get the data as a `DataBatch`.
/// The caller (service layer) is responsible for storing the data.
///
/// # Example
///
/// ```rust,ignore
/// // Service layer calls poll_once() and handles storage
/// loop {
///     let batch = channel.poll_once().await?;
///     store.write_batch(channel_id, &batch).await?;
/// }
/// ```
pub struct ModbusChannel {
    config: ModbusChannelConfig,
    /// Channel identifier for logging.
    channel_id: u32,
    /// Client wrapped in Arc<Mutex> for shared access from polling task.
    /// Uses ModbusClientWrapper to support both TCP and RTU transports.
    client: Arc<Mutex<Option<ModbusClientWrapper>>>,
    state: Arc<std::sync::RwLock<ConnectionState>>,
    diagnostics: Arc<RwLock<ChannelDiagnostics>>,

    // === Polling support ===
    /// Flag to control polling loop exit
    is_polling: Arc<AtomicBool>,
    /// Polling task handle (legacy support)
    polling_handle: Option<JoinHandle<()>>,
    /// Pre-grouped points by (slave_id, function_code)
    grouped_points: Arc<RwLock<GroupedPoints>>,
    /// Polling interval in milliseconds
    polling_interval_ms: u64,

    // === Reconnect support ===
    /// Reconnect state for automatic recovery
    reconnect_state: Arc<std::sync::RwLock<ReconnectState>>,

    // === Command batching ===
    /// Command batcher for optimizing write operations
    command_batcher: Arc<Mutex<CommandBatcher>>,

    // === Logging ===
    /// Logging context for channel events
    log_context: Arc<LogContext>,
}

#[derive(Debug, Default)]
struct ChannelDiagnostics {
    read_count: u64,
    write_count: u64,
    error_count: u64,
    last_error: Option<String>,
}

/// Default polling interval in milliseconds
const DEFAULT_POLLING_INTERVAL_MS: u64 = 1000;

impl ModbusChannel {
    /// Create a new Modbus channel.
    ///
    /// # Arguments
    /// - `config`: Channel configuration including address and points
    /// - `channel_id`: Unique identifier for this channel (used in logs)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let config = ModbusChannelConfig::tcp("192.168.1.100:502")
    ///     .with_points(points);
    /// let mut channel = ModbusChannel::new(config, 1);
    /// channel.connect().await?;
    ///
    /// // Poll and get data (service layer stores it)
    /// let batch = channel.poll_once().await?;
    /// ```
    pub fn new(config: ModbusChannelConfig, channel_id: u32) -> Self {
        Self {
            config,
            channel_id,
            client: Arc::new(Mutex::new(None)),
            state: Arc::new(std::sync::RwLock::new(ConnectionState::Disconnected)),
            diagnostics: Arc::new(RwLock::new(ChannelDiagnostics::default())),
            is_polling: Arc::new(AtomicBool::new(false)),
            polling_handle: None,
            grouped_points: Arc::new(RwLock::new(HashMap::new())),
            polling_interval_ms: DEFAULT_POLLING_INTERVAL_MS,
            reconnect_state: Arc::new(std::sync::RwLock::new(ReconnectState::default())),
            command_batcher: Arc::new(Mutex::new(CommandBatcher::new())),
            log_context: Arc::new(LogContext::new(channel_id)),
        }
    }

    /// Set polling interval.
    pub fn with_polling_interval(mut self, interval_ms: u64) -> Self {
        self.polling_interval_ms = interval_ms;
        self
    }

    /// Set connection state (internal helper).
    fn set_state(&self, state: ConnectionState) {
        if let Ok(mut s) = self.state.write() {
            *s = state;
        }
    }

    /// Get connection state.
    fn get_state(&self) -> ConnectionState {
        self.state
            .read()
            .map(|s| *s)
            .unwrap_or(ConnectionState::Error)
    }

    /// Get the point configurations.
    pub fn points(&self) -> &[PointConfig] {
        &self.config.points
    }

    /// Read a single Modbus address and convert to DataPoint.
    #[allow(dead_code)]
    async fn read_modbus_point(&self, point: &PointConfig) -> Result<DataPoint> {
        let mut client_guard = self.client.lock().await;
        let client = client_guard.as_mut().ok_or(GatewayError::NotConnected)?;

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
                decode_registers(
                    &regs,
                    modbus_addr.format,
                    modbus_addr.byte_order,
                    modbus_addr.bit_position,
                )?
            }
            4 => {
                // Read input registers (FC04)
                let count = modbus_addr.format.register_count();
                let regs = client
                    .read_04(modbus_addr.slave_id, modbus_addr.register, count)
                    .await
                    .map_err(|e| GatewayError::Protocol(e.to_string()))?;
                decode_registers(
                    &regs,
                    modbus_addr.format,
                    modbus_addr.byte_order,
                    modbus_addr.bit_position,
                )?
            }
            _ => {
                return Err(GatewayError::Unsupported(format!(
                    "Function code {} not supported for read",
                    modbus_addr.function_code
                )))
            }
        };

        // Apply transform
        let transformed_value = apply_transform(value, &point.transform);

        Ok(DataPoint::new(point.id, point.data_type, transformed_value))
    }

    /// Record an error in diagnostics.
    async fn record_error(&self, error: &str) {
        let mut diag = self.diagnostics.write().await;
        diag.error_count += 1;
        diag.last_error = Some(error.to_string());
    }

    /// Pre-group points by (slave_id, function_code) for polling optimization.
    ///
    /// Only Telemetry and Signal points are included (Control/Adjustment are not polled).
    async fn group_points_for_polling(&self) {
        let mut groups: GroupedPoints = HashMap::new();

        for point in &self.config.points {
            // Only poll Telemetry and Signal points
            if !matches!(point.data_type, DataType::Telemetry | DataType::Signal) {
                continue;
            }

            // Extract Modbus address
            if let ProtocolAddress::Modbus(addr) = &point.address {
                let key = (addr.slave_id, addr.function_code);
                groups.entry(key).or_default().push(point.clone());
            }
        }

        let mut guard = self.grouped_points.write().await;
        *guard = groups;

        debug!(
            "[{}] grouped {} points into {} groups",
            self.config.address,
            self.config.points.len(),
            guard.len()
        );
    }

    /// Read a group of points with the same slave_id and function_code.
    ///
    /// Uses batch reading optimization: consecutive registers are read in single requests.
    /// Returns a list of (point_id, DataPoint) tuples for successfully read points.
    async fn read_point_group(
        client: &mut ModbusClientWrapper,
        points: &[PointConfig],
        max_batch_size: u16,
        max_gap: u16,
    ) -> Vec<(u32, DataPoint)> {
        if points.is_empty() {
            return Vec::new();
        }

        // Get slave_id and function_code from first point (all points in group share these)
        let (slave_id, function_code) = match &points[0].address {
            ProtocolAddress::Modbus(addr) => (addr.slave_id, addr.function_code),
            _ => return Vec::new(),
        };

        // For coils/discrete inputs (FC01/FC02), read individually (simpler logic)
        if function_code == 1 || function_code == 2 {
            return Self::read_coils_individually(client, points, slave_id, function_code).await;
        }

        // For registers (FC03/FC04), use batch optimization
        Self::read_registers_batched(
            client,
            points,
            slave_id,
            function_code,
            max_batch_size,
            max_gap,
        )
        .await
    }

    /// Read coils or discrete inputs individually (FC01/FC02).
    async fn read_coils_individually(
        client: &mut ModbusClientWrapper,
        points: &[PointConfig],
        slave_id: u8,
        function_code: u8,
    ) -> Vec<(u32, DataPoint)> {
        let mut results = Vec::with_capacity(points.len());

        for point in points {
            let modbus_addr = match &point.address {
                ProtocolAddress::Modbus(addr) => addr,
                _ => continue,
            };

            let value_result = match function_code {
                1 => client
                    .read_01(slave_id, modbus_addr.register, 1)
                    .await
                    .map(|coils| Value::Bool(coils.first().copied().unwrap_or(false))),
                2 => client
                    .read_02(slave_id, modbus_addr.register, 1)
                    .await
                    .map(|inputs| Value::Bool(inputs.first().copied().unwrap_or(false))),
                _ => continue,
            };

            if let Ok(value) = value_result {
                let transformed = apply_transform(value, &point.transform);
                results.push((
                    point.id,
                    DataPoint::new(point.id, point.data_type, transformed),
                ));
            }
        }

        results
    }

    /// Read registers in batches (FC03/FC04).
    ///
    /// Groups consecutive registers (within max_gap) and reads them in single requests.
    async fn read_registers_batched(
        client: &mut ModbusClientWrapper,
        points: &[PointConfig],
        slave_id: u8,
        function_code: u8,
        max_batch_size: u16,
        max_gap: u16,
    ) -> Vec<(u32, DataPoint)> {
        // Sort points by register address
        let mut sorted_points: Vec<_> = points
            .iter()
            .filter_map(|p| {
                if let ProtocolAddress::Modbus(addr) = &p.address {
                    Some((addr.register, addr.format.register_count(), p))
                } else {
                    None
                }
            })
            .collect();
        sorted_points.sort_by_key(|(addr, _, _)| *addr);

        if sorted_points.is_empty() {
            return Vec::new();
        }

        // Build segments of consecutive registers
        let segments = Self::build_register_segments(&sorted_points, max_gap, max_batch_size);

        // Read each segment
        let mut results = Vec::with_capacity(points.len());

        for segment in segments {
            let batch_result =
                Self::read_register_segment(client, slave_id, function_code, &segment).await;

            match batch_result {
                Ok(batch_results) => results.extend(batch_results),
                Err(e) => {
                    debug!(
                        "Batch read failed for segment @{}: {}",
                        segment.start_address, e
                    );
                }
            }
        }

        results
    }

    /// Build segments of consecutive registers for batch reading.
    fn build_register_segments<'a>(
        sorted_points: &[(u16, u16, &'a PointConfig)],
        max_gap: u16,
        max_batch_size: u16,
    ) -> Vec<RegisterSegment<'a>> {
        let mut segments = Vec::new();
        let mut current_segment: Option<RegisterSegment> = None;

        for &(addr, count, point) in sorted_points {
            match &mut current_segment {
                None => {
                    // Start new segment
                    current_segment = Some(RegisterSegment {
                        start_address: addr,
                        end_address: addr + count,
                        points: vec![(addr, count, point)],
                    });
                }
                Some(seg) => {
                    let gap = addr.saturating_sub(seg.end_address);
                    let new_total = (addr + count).saturating_sub(seg.start_address);

                    // Check if we can extend current segment
                    if gap <= max_gap && new_total <= max_batch_size {
                        seg.end_address = addr + count;
                        seg.points.push((addr, count, point));
                    } else {
                        // Segment complete, start new one
                        segments.push(current_segment.take().unwrap());
                        current_segment = Some(RegisterSegment {
                            start_address: addr,
                            end_address: addr + count,
                            points: vec![(addr, count, point)],
                        });
                    }
                }
            }
        }

        // Add last segment
        if let Some(seg) = current_segment {
            segments.push(seg);
        }

        segments
    }

    /// Read a segment of consecutive registers and decode individual points.
    #[allow(clippy::needless_lifetimes)]
    async fn read_register_segment<'a>(
        client: &mut ModbusClientWrapper,
        slave_id: u8,
        function_code: u8,
        segment: &RegisterSegment<'a>,
    ) -> std::result::Result<Vec<(u32, DataPoint)>, voltage_modbus::ModbusError> {
        let total_registers = segment.end_address - segment.start_address;

        // Read all registers in segment
        let registers = match function_code {
            3 => {
                client
                    .read_03(slave_id, segment.start_address, total_registers)
                    .await?
            }
            4 => {
                client
                    .read_04(slave_id, segment.start_address, total_registers)
                    .await?
            }
            _ => return Ok(Vec::new()),
        };

        // Decode each point from the register buffer
        let mut results = Vec::with_capacity(segment.points.len());

        for &(addr, count, point) in &segment.points {
            let offset = (addr - segment.start_address) as usize;
            let end = offset + count as usize;

            if end <= registers.len() {
                let point_regs = &registers[offset..end];

                if let ProtocolAddress::Modbus(modbus_addr) = &point.address {
                    if let Ok(value) = decode_registers(
                        point_regs,
                        modbus_addr.format,
                        modbus_addr.byte_order,
                        modbus_addr.bit_position,
                    ) {
                        let transformed = apply_transform(value, &point.transform);
                        results.push((
                            point.id,
                            DataPoint::new(point.id, point.data_type, transformed),
                        ));
                    }
                }
            }
        }

        Ok(results)
    }
}

impl HasMetadata for ModbusChannel {
    fn metadata() -> DriverMetadata {
        DriverMetadata {
            name: "modbus_tcp",
            display_name: "Modbus TCP",
            description: "Industrial Modbus TCP protocol for reading/writing registers and coils.",
            is_recommended: true,
            example_config: serde_json::json!({
                "host": "192.168.1.100",
                "port": 502,
                "slave_id": 1,
                "connect_timeout_ms": 5000,
                "read_timeout_ms": 3000,
                "polling_interval_ms": 1000,
                "max_batch_size": 125,
                "max_reconnect_attempts": 5
            }),
            parameters: vec![
                ParameterMetadata::required(
                    "host",
                    "Host",
                    "Modbus device IP address or hostname",
                    ParameterType::String,
                ),
                ParameterMetadata::optional(
                    "port",
                    "Port",
                    "Modbus TCP port",
                    ParameterType::Integer,
                    serde_json::json!(502),
                ),
                ParameterMetadata::optional(
                    "slave_id",
                    "Slave ID",
                    "Modbus slave/unit ID (1-247)",
                    ParameterType::Integer,
                    serde_json::json!(1),
                ),
                ParameterMetadata::optional(
                    "connect_timeout_ms",
                    "Connect Timeout (ms)",
                    "Connection timeout in milliseconds",
                    ParameterType::Integer,
                    serde_json::json!(5000),
                ),
                ParameterMetadata::optional(
                    "read_timeout_ms",
                    "Read Timeout (ms)",
                    "Read operation timeout in milliseconds",
                    ParameterType::Integer,
                    serde_json::json!(3000),
                ),
                ParameterMetadata::optional(
                    "polling_interval_ms",
                    "Polling Interval (ms)",
                    "Polling interval in milliseconds",
                    ParameterType::Integer,
                    serde_json::json!(1000),
                ),
                ParameterMetadata::optional(
                    "max_batch_size",
                    "Max Batch Size",
                    "Maximum registers per batch read (max 125)",
                    ParameterType::Integer,
                    serde_json::json!(125),
                ),
                ParameterMetadata::optional(
                    "max_reconnect_attempts",
                    "Max Reconnect Attempts",
                    "Maximum reconnection attempts before giving up",
                    ParameterType::Integer,
                    serde_json::json!(5),
                ),
            ],
        }
    }
}

/// A segment of consecutive registers to be read in one batch.
struct RegisterSegment<'a> {
    start_address: u16,
    end_address: u16,
    points: Vec<(u16, u16, &'a PointConfig)>, // (address, count, point)
}

// ============================================================
// Command Batching Support
// ============================================================

impl ModbusChannel {
    /// Queue an adjustment command for batched execution.
    ///
    /// Commands are collected and executed when:
    /// - Batch window expires (20ms), or
    /// - Maximum batch size reached (100 commands)
    ///
    /// Call `check_and_execute_batch()` periodically to trigger execution.
    pub async fn queue_adjustment(&self, adj: &AdjustmentCommand) -> Result<()> {
        // Find point config
        let point = self
            .config
            .points
            .iter()
            .find(|p| p.id == adj.id && p.data_type == DataType::Adjustment)
            .ok_or_else(|| GatewayError::InvalidAddress(format!("Point {} not found", adj.id)))?;

        let modbus_addr = match &point.address {
            ProtocolAddress::Modbus(addr) => addr.clone(),
            _ => {
                return Err(GatewayError::InvalidAddress(
                    "Non-Modbus address type".into(),
                ))
            }
        };

        // Apply reverse transform to get raw value
        let raw_value = reverse_transform(adj.value, &point.transform)?;

        // Create batch command
        let batch_cmd = BatchCommand {
            point_id: adj.id,
            value: Value::Float(raw_value),
            slave_id: modbus_addr.slave_id,
            function_code: if modbus_addr.format.register_count() > 1 {
                16
            } else {
                6
            },
            register_address: modbus_addr.register,
            data_format: modbus_addr.format,
            byte_order: modbus_addr.byte_order,
        };

        // Add to batcher
        let mut batcher = self.command_batcher.lock().await;
        batcher.add_command(batch_cmd);

        Ok(())
    }

    /// Check if pending commands should be executed, and execute if so.
    ///
    /// Returns `Some(WriteResult)` if commands were executed, `None` otherwise.
    /// Call this periodically (e.g., in a polling loop) to flush batched commands.
    pub async fn check_and_execute_batch(&mut self) -> Result<Option<WriteResult>> {
        let should_execute = {
            let batcher = self.command_batcher.lock().await;
            batcher.should_execute() && batcher.pending_count() > 0
        };

        if should_execute {
            Ok(Some(self.execute_batched_commands().await?))
        } else {
            Ok(None)
        }
    }

    /// Execute all pending batched commands with FC16 optimization.
    ///
    /// This method takes all pending commands from the batcher and executes them.
    /// For commands with consecutive register addresses on the same slave,
    /// it uses FC16 (Write Multiple Registers) to batch them into a single request.
    pub async fn execute_batched_commands(&mut self) -> Result<WriteResult> {
        let batches = {
            let mut batcher = self.command_batcher.lock().await;
            batcher.take_commands()
        };

        if batches.is_empty() {
            return Ok(WriteResult {
                success_count: 0,
                failures: Vec::new(),
            });
        }

        let mut success_count = 0;
        let mut failures = Vec::new();

        // Lock client for execution
        let mut client_guard = self.client.lock().await;
        let client = match client_guard.as_mut() {
            Some(c) => c,
            None => return Err(GatewayError::NotConnected),
        };

        for ((slave_id, fc), commands) in batches {
            // FC16 optimization: merge consecutive addresses into single write
            if fc == 16 && commands.len() > 1 && CommandBatcher::are_strictly_consecutive(&commands)
            {
                // Merge all commands into single FC16 request
                match self
                    .execute_merged_fc16(client, slave_id, &commands, &mut failures)
                    .await
                {
                    Ok(count) => success_count += count,
                    Err(e) => {
                        // If merged write fails, record for all commands
                        for cmd in &commands {
                            failures.push((cmd.point_id, e.to_string()));
                        }
                    }
                }
            } else {
                // Execute commands individually
                for cmd in commands {
                    let result = match cmd.data_format {
                        DataFormat::UInt16 | DataFormat::Int16 | DataFormat::Bool => {
                            let value = cmd.value.as_f64().unwrap_or(0.0) as u16;
                            client
                                .write_06(cmd.slave_id, cmd.register_address, value)
                                .await
                        }
                        DataFormat::UInt32 | DataFormat::Int32 | DataFormat::Float32 => {
                            let raw_value = cmd.value.as_f64().unwrap_or(0.0);
                            match encode_value(raw_value, cmd.data_format, cmd.byte_order) {
                                Ok(regs) => {
                                    client
                                        .write_10(cmd.slave_id, cmd.register_address, &regs)
                                        .await
                                }
                                Err(e) => {
                                    failures.push((cmd.point_id, e.to_string()));
                                    continue;
                                }
                            }
                        }
                        _ => {
                            failures.push((cmd.point_id, "Unsupported format".into()));
                            continue;
                        }
                    };

                    match result {
                        Ok(_) => success_count += 1,
                        Err(e) => failures.push((cmd.point_id, e.to_string())),
                    }
                }
            }
        }

        // Update diagnostics
        drop(client_guard);
        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += success_count as u64;
            if !failures.is_empty() {
                diag.error_count += failures.len() as u64;
                if let Some((_, err)) = failures.last() {
                    diag.last_error = Some(err.clone());
                }
            }
        }

        Ok(WriteResult {
            success_count,
            failures,
        })
    }

    /// Execute merged FC16 (Write Multiple Registers) for consecutive addresses.
    ///
    /// This combines multiple single-register writes into one multi-register write,
    /// significantly reducing network round-trips when writing adjacent registers.
    async fn execute_merged_fc16(
        &self,
        client: &mut ModbusClientWrapper,
        slave_id: u8,
        commands: &[BatchCommand],
        failures: &mut Vec<(u32, String)>,
    ) -> std::result::Result<usize, voltage_modbus::ModbusError> {
        // Sort commands by register address
        let mut sorted = commands.to_vec();
        sorted.sort_by_key(|c| c.register_address);

        // Build merged register buffer
        let start_addr = sorted[0].register_address;
        let mut registers = Vec::new();

        for cmd in &sorted {
            let raw_value = cmd.value.as_f64().unwrap_or(0.0);
            match encode_value(raw_value, cmd.data_format, cmd.byte_order) {
                Ok(regs) => registers.extend(regs),
                Err(e) => {
                    failures.push((cmd.point_id, e.to_string()));
                    // Continue building buffer, but record failure
                }
            }
        }

        if registers.is_empty() {
            return Ok(0);
        }

        // Execute merged FC16
        debug!(
            "FC16 merge: {} commands → {} registers starting at {}",
            sorted.len(),
            registers.len(),
            start_addr
        );

        client.write_10(slave_id, start_addr, &registers).await?;

        // Count successful writes (excluding any that failed encoding)
        Ok(sorted.len() - failures.len())
    }
}

impl ProtocolCapabilities for ModbusChannel {
    fn name(&self) -> &'static str {
        match self.config.connection_mode {
            ConnectionMode::Tcp => "Modbus TCP",
            #[cfg(feature = "modbus")]
            ConnectionMode::Rtu => "Modbus RTU",
        }
    }

    fn supported_modes(&self) -> &[CommunicationMode] {
        &[CommunicationMode::Polling]
    }

    fn version(&self) -> &'static str {
        "1.0"
    }
}

impl LoggableProtocol for ModbusChannel {
    fn set_log_handler(&mut self, handler: Arc<dyn ChannelLogHandler>) {
        // Create a new LogContext with the handler
        let new_ctx = LogContext::new(self.channel_id)
            .with_handler(handler)
            .with_config(self.log_context.config().clone());
        self.log_context = Arc::new(new_ctx);
    }

    fn set_log_config(&mut self, config: ChannelLogConfig) {
        // We need to recreate the LogContext since it's behind Arc
        // For simplicity, we use Arc::make_mut pattern or recreate
        if let Some(ctx) = Arc::get_mut(&mut self.log_context) {
            ctx.set_config(config);
        } else {
            // Clone and update
            let mut new_ctx = (*self.log_context).clone();
            new_ctx.set_config(config);
            self.log_context = Arc::new(new_ctx);
        }
    }

    fn log_config(&self) -> &ChannelLogConfig {
        self.log_context.config()
    }
}

impl Protocol for ModbusChannel {
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

impl ProtocolClient for ModbusChannel {
    async fn connect(&mut self) -> Result<()> {
        let start_time = std::time::Instant::now();
        let old_state = self.get_state();
        self.set_state(ConnectionState::Connecting);

        // Log state change
        self.log_context
            .log_state_changed(old_state, ConnectionState::Connecting)
            .await;

        // Get endpoint for logging
        let endpoint = match self.config.connection_mode {
            ConnectionMode::Tcp => self.config.address.clone(),
            #[cfg(feature = "modbus")]
            ConnectionMode::Rtu => {
                format!("{}@{}", self.config.rtu_device, self.config.baud_rate)
            }
        };

        // Create client based on connection mode
        let connect_result = match self.config.connection_mode {
            ConnectionMode::Tcp => {
                // TCP connection
                match ModbusTcpClient::from_address(
                    &self.config.address,
                    self.config.connect_timeout,
                )
                .await
                {
                    Ok(client) => Ok(ModbusClientWrapper::Tcp(client)),
                    Err(e) => Err(GatewayError::Connection(e.to_string())),
                }
            }
            #[cfg(feature = "modbus")]
            ConnectionMode::Rtu => {
                // RTU serial connection
                match ModbusRtuClient::new(&self.config.rtu_device, self.config.baud_rate) {
                    Ok(client) => Ok(ModbusClientWrapper::Rtu(client)),
                    Err(e) => Err(GatewayError::Connection(e.to_string())),
                }
            }
        };

        let duration_ms = start_time.elapsed().as_millis() as u64;

        match connect_result {
            Ok(wrapper) => {
                let mut client_guard = self.client.lock().await;
                *client_guard = Some(wrapper);
                self.set_state(ConnectionState::Connected);

                // Log successful connection
                self.log_context.log_connected(&endpoint, duration_ms).await;
                self.log_context
                    .log_state_changed(ConnectionState::Connecting, ConnectionState::Connected)
                    .await;

                // Pre-group points after successful connection
                self.group_points_for_polling().await;
                Ok(())
            }
            Err(e) => {
                self.set_state(ConnectionState::Error);
                self.record_error(&e.to_string()).await;

                // Log error
                self.log_context
                    .log_error(&e.to_string(), ErrorContext::Connection)
                    .await;
                self.log_context
                    .log_state_changed(ConnectionState::Connecting, ConnectionState::Error)
                    .await;

                Err(e)
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        let old_state = self.get_state();

        // Stop polling first if running
        if self.is_polling.load(Ordering::SeqCst) {
            self.stop_polling().await?;
        }

        let mut client_guard = self.client.lock().await;
        if let Some(mut client) = client_guard.take() {
            let _ = client.close().await;
        }
        self.set_state(ConnectionState::Disconnected);

        // Log disconnection
        self.log_context.log_disconnected(None).await;
        self.log_context
            .log_state_changed(old_state, ConnectionState::Disconnected)
            .await;

        Ok(())
    }

    async fn poll_once(&mut self) -> Result<DataBatch> {
        let start_time = std::time::Instant::now();

        // Ensure points are grouped for efficient polling
        if self.grouped_points.read().await.is_empty() && !self.config.points.is_empty() {
            self.group_points_for_polling().await;
        }

        // Acquire client lock
        let mut client_guard = self.client.lock().await;
        let client = match client_guard.as_mut() {
            Some(c) => c,
            None => {
                self.log_context
                    .log_error("Not connected", ErrorContext::Polling)
                    .await;
                return Err(GatewayError::NotConnected);
            }
        };

        // Read all point groups - clone to release lock before async I/O
        let groups: Vec<_> = {
            let g = self.grouped_points.read().await;
            g.iter().map(|(k, v)| (*k, v.clone())).collect()
        }; // Lock released here

        let mut batch = DataBatch::default();
        let mut read_count = 0u64;
        let mut error_count = 0u64;

        for ((_slave_id, _fc), points) in groups.iter() {
            let results = Self::read_point_group(
                client,
                points,
                self.config.max_batch_size,
                self.config.max_gap,
            )
            .await;

            if results.is_empty() && !points.is_empty() {
                // Read returned no results - possible communication error
                error_count += 1;
            }

            for (_point_id, data_point) in results {
                batch.add(data_point);
                read_count += 1;
            }
        }

        // Update diagnostics
        {
            let mut diag = self.diagnostics.write().await;
            diag.read_count += read_count;
            diag.error_count += error_count;
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        debug!(
            "[{}] poll_once: read {} points",
            self.config.address,
            batch.len()
        );

        // Log poll cycle
        self.log_context
            .log_poll_cycle(
                batch.clone(),
                duration_ms,
                read_count as usize,
                error_count as usize,
            )
            .await;

        Ok(batch)
    }

    async fn write_control(&mut self, commands: &[ControlCommand]) -> Result<WriteResult> {
        let start_time = std::time::Instant::now();
        let commands_vec = commands.to_vec();
        let mut success_count = 0;
        let mut failures = Vec::new();
        let mut errors_to_record = Vec::new();

        // Lock client for the entire operation
        let mut client_guard = self.client.lock().await;
        let client = match client_guard.as_mut() {
            Some(c) => c,
            None => {
                let err = GatewayError::NotConnected;
                self.log_context
                    .log_control_write(
                        commands_vec,
                        Err(err.to_string()),
                        start_time.elapsed().as_millis() as u64,
                    )
                    .await;
                return Err(err);
            }
        };

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
                    failures.push((cmd.id, "Point not found".into()));
                    continue;
                }
            };

            let modbus_addr = match &point.address {
                ProtocolAddress::Modbus(addr) => addr.clone(),
                _ => {
                    failures.push((cmd.id, "Invalid address type".into()));
                    continue;
                }
            };

            // Apply reverse transform for control (boolean inversion)
            let value = point.transform.apply_bool(cmd.value);

            // Write based on function_code from mapping
            let result = match modbus_addr.function_code {
                5 => {
                    // FC05: Write Single Coil (bool)
                    client
                        .write_05(modbus_addr.slave_id, modbus_addr.register, value)
                        .await
                }
                6 => {
                    // FC06: Write Single Register (u16)
                    // Convert bool to 0/1
                    let reg_value = if value { 1u16 } else { 0u16 };
                    client
                        .write_06(modbus_addr.slave_id, modbus_addr.register, reg_value)
                        .await
                }
                16 => {
                    // FC16: Write Multiple Registers
                    let reg_value = if value { 1u16 } else { 0u16 };
                    client
                        .write_10(modbus_addr.slave_id, modbus_addr.register, &[reg_value])
                        .await
                }
                fc => {
                    failures.push((
                        cmd.id,
                        format!("Unsupported function code {} for control", fc),
                    ));
                    continue;
                }
            };

            match result {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    failures.push((cmd.id, err_msg.clone()));
                    errors_to_record.push(err_msg);
                }
            }
        }

        // Drop client lock before acquiring diagnostics lock
        drop(client_guard);

        // Record errors and update diagnostics after loop
        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += success_count as u64;
            if let Some(err) = errors_to_record.last() {
                diag.error_count += errors_to_record.len() as u64;
                diag.last_error = Some(err.clone());
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;
        let result = WriteResult {
            success_count,
            failures,
        };

        // Log control write
        self.log_context
            .log_control_write(commands_vec, Ok(result.clone()), duration_ms)
            .await;

        Ok(result)
    }

    async fn write_adjustment(&mut self, adjustments: &[AdjustmentCommand]) -> Result<WriteResult> {
        let start_time = std::time::Instant::now();
        let adjustments_vec = adjustments.to_vec();
        let mut success_count = 0;
        let mut failures = Vec::new();
        let mut errors_to_record = Vec::new();

        // Lock client for the entire operation
        let mut client_guard = self.client.lock().await;
        let client = match client_guard.as_mut() {
            Some(c) => c,
            None => {
                let err = GatewayError::NotConnected;
                self.log_context
                    .log_adjustment_write(
                        adjustments_vec,
                        Err(err.to_string()),
                        start_time.elapsed().as_millis() as u64,
                    )
                    .await;
                return Err(err);
            }
        };

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
                    failures.push((adj.id, "Point not found".into()));
                    continue;
                }
            };

            let modbus_addr = match &point.address {
                ProtocolAddress::Modbus(addr) => addr.clone(),
                _ => {
                    failures.push((adj.id, "Invalid address type".into()));
                    continue;
                }
            };

            // Apply reverse transform to get raw value
            let raw_value = match reverse_transform(adj.value, &point.transform) {
                Ok(v) => v,
                Err(e) => {
                    failures.push((adj.id, e.to_string()));
                    continue;
                }
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
                    failures.push((adj.id, "Unsupported format for write".into()));
                    continue;
                }
            };

            match result {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    let err_msg = e.to_string();
                    failures.push((adj.id, err_msg.clone()));
                    errors_to_record.push(err_msg);
                }
            }
        }

        // Drop client lock before acquiring diagnostics lock
        drop(client_guard);

        // Record errors and update diagnostics after loop
        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += success_count as u64;
            if let Some(err) = errors_to_record.last() {
                diag.error_count += errors_to_record.len() as u64;
                diag.last_error = Some(err.clone());
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;
        let result = WriteResult {
            success_count,
            failures,
        };

        // Log adjustment write
        self.log_context
            .log_adjustment_write(adjustments_vec, Ok(result.clone()), duration_ms)
            .await;

        Ok(result)
    }

    /// Start background polling task (legacy method).
    ///
    /// **NOTE**: This is a legacy method kept for backward compatibility.
    /// Prefer using `poll_once()` with an external loop managed by the service layer.
    ///
    /// In the new architecture, this method only polls and updates diagnostics.
    /// The caller should use `poll_once()` instead and handle data storage themselves.
    async fn start_polling(&mut self, config: PollingConfig) -> Result<()> {
        // Check if already polling
        if self.is_polling.load(Ordering::SeqCst) {
            warn!("[{}] polling already running", self.config.address);
            return Ok(());
        }

        // Set polling flag
        self.is_polling.store(true, Ordering::SeqCst);

        // Ensure points are grouped
        self.group_points_for_polling().await;

        // Use config interval if provided, otherwise use default
        let interval_ms = if config.interval_ms > 0 {
            config.interval_ms
        } else {
            self.polling_interval_ms
        };

        // Clone necessary state for the polling task
        let client = Arc::clone(&self.client);
        let grouped_points = Arc::clone(&self.grouped_points);
        let is_polling = Arc::clone(&self.is_polling);
        let diagnostics = Arc::clone(&self.diagnostics);
        let state = Arc::clone(&self.state);
        let reconnect_state = Arc::clone(&self.reconnect_state);

        // Batch read configuration
        let max_batch_size = self.config.max_batch_size;
        let max_gap = self.config.max_gap;

        // Reconnect configuration
        let reconnect_config = self.config.reconnect.clone();
        let target_address = self.config.address.clone();
        let connect_timeout = self.config.connect_timeout;
        let connection_mode = self.config.connection_mode;
        #[cfg(feature = "modbus")]
        let rtu_device = self.config.rtu_device.clone();
        #[cfg(feature = "modbus")]
        let baud_rate = self.config.baud_rate;

        // Build display name for logging
        let display_name = match connection_mode {
            ConnectionMode::Tcp => target_address.clone(),
            #[cfg(feature = "modbus")]
            ConnectionMode::Rtu => format!("{}@{}", rtu_device, baud_rate),
        };

        info!(
            "[{}] starting polling with {}ms interval (legacy mode - data not stored)",
            display_name, interval_ms
        );

        // Spawn polling task
        let handle = tokio::spawn(async move {
            let mut poll_interval = interval(Duration::from_millis(interval_ms));
            let mut consecutive_zero_cycles = 0u32;

            while is_polling.load(Ordering::SeqCst) {
                poll_interval.tick().await;

                // Skip if not polling anymore
                if !is_polling.load(Ordering::SeqCst) {
                    break;
                }

                // Check connection and attempt reconnect if needed
                let mut client_guard = client.lock().await;

                if client_guard.is_none() {
                    // Connection lost - check if we should attempt reconnect
                    let should_reconnect = {
                        let mut rs = reconnect_state.write().unwrap();
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64;

                        let cooldown_elapsed =
                            now_ms - rs.last_attempt_ms >= reconnect_config.cooldown_ms;
                        let attempts_ok = reconnect_config.max_attempts == 0
                            || rs.attempts < reconnect_config.max_attempts;

                        if cooldown_elapsed && attempts_ok {
                            rs.attempts += 1;
                            rs.last_attempt_ms = now_ms;
                            true
                        } else {
                            false
                        }
                    };

                    if should_reconnect {
                        let attempt = reconnect_state.read().unwrap().attempts;
                        info!(
                            "[{}] attempting reconnect (attempt {})",
                            display_name, attempt
                        );

                        // Attempt reconnect based on connection mode
                        let reconnect_result: std::result::Result<ModbusClientWrapper, String> =
                            match connection_mode {
                                ConnectionMode::Tcp => {
                                    match ModbusTcpClient::from_address(
                                        &target_address,
                                        connect_timeout,
                                    )
                                    .await
                                    {
                                        Ok(client) => Ok(ModbusClientWrapper::Tcp(client)),
                                        Err(e) => Err(e.to_string()),
                                    }
                                }
                                #[cfg(feature = "modbus")]
                                ConnectionMode::Rtu => {
                                    match ModbusRtuClient::new(&rtu_device, baud_rate) {
                                        Ok(client) => Ok(ModbusClientWrapper::Rtu(client)),
                                        Err(e) => Err(e.to_string()),
                                    }
                                }
                            };

                        match reconnect_result {
                            Ok(new_client) => {
                                info!("[{}] reconnected successfully", display_name);
                                *client_guard = Some(new_client);
                                if let Ok(mut s) = state.write() {
                                    *s = ConnectionState::Connected;
                                }
                                if let Ok(mut rs) = reconnect_state.write() {
                                    rs.attempts = 0;
                                    rs.consecutive_zero_cycles = 0;
                                }
                                consecutive_zero_cycles = 0;
                            }
                            Err(e) => {
                                warn!("[{}] reconnect failed: {}", display_name, e);
                            }
                        }
                    }

                    if client_guard.is_none() {
                        continue;
                    }
                }

                let client_ref = client_guard.as_mut().unwrap();

                // Read all point groups - clone to release lock before async I/O
                let groups: Vec<_> = {
                    let g = grouped_points.read().await;
                    g.iter().map(|(k, v)| (*k, v.clone())).collect()
                }; // Lock released here

                let mut batch = DataBatch::default();
                let mut read_count = 0u64;
                let mut error_count = 0u64;

                for ((_slave_id, _fc), points) in groups.iter() {
                    let results =
                        Self::read_point_group(client_ref, points, max_batch_size, max_gap).await;

                    if results.is_empty() && !points.is_empty() {
                        error_count += 1;
                    }

                    for (_point_id, data_point) in results {
                        batch.add(data_point);
                        read_count += 1;
                    }
                }

                drop(client_guard);

                // Track zero-data cycles for reconnect trigger
                if batch.is_empty() && !groups.is_empty() {
                    consecutive_zero_cycles += 1;

                    if consecutive_zero_cycles >= reconnect_config.zero_data_threshold {
                        warn!(
                            "[{}] {} consecutive zero-data cycles, triggering reconnect",
                            display_name, consecutive_zero_cycles
                        );

                        if let Ok(mut s) = state.write() {
                            *s = ConnectionState::Disconnected;
                        }
                        let mut client_guard = client.lock().await;
                        if let Some(c) = client_guard.take() {
                            drop(c);
                        }
                        consecutive_zero_cycles = 0;
                    }
                } else if !batch.is_empty() {
                    consecutive_zero_cycles = 0;
                    debug!(
                        "[{}] polled {} points (legacy mode)",
                        display_name,
                        batch.len()
                    );
                }

                // Update diagnostics
                {
                    let mut diag = diagnostics.write().await;
                    diag.read_count += read_count;
                    diag.error_count += error_count;
                }
            }

            info!("[{}] polling task stopped", display_name);
        });

        self.polling_handle = Some(handle);
        Ok(())
    }

    async fn stop_polling(&mut self) -> Result<()> {
        if !self.is_polling.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!("[{}] stopping polling", self.config.address);

        // Signal polling task to stop
        self.is_polling.store(false, Ordering::SeqCst);

        // Wait for task to complete (with timeout)
        if let Some(handle) = self.polling_handle.take() {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !handle.is_finished() {
                handle.abort();
            }
        }

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
fn reverse_transform(
    value: f64,
    transform: &crate::core::point::TransformConfig,
) -> crate::Result<f64> {
    transform.reverse_apply(value)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let config = ModbusChannelConfig::tcp("127.0.0.1:502");
        let channel = ModbusChannel::new(config, 1);

        assert_eq!(channel.name(), "Modbus TCP");
        assert_eq!(channel.supported_modes(), &[CommunicationMode::Polling]);
    }

    #[test]
    fn test_polling_interval_builder() {
        let config = ModbusChannelConfig::tcp("127.0.0.1:502");
        let channel = ModbusChannel::new(config, 1).with_polling_interval(500);

        assert_eq!(channel.polling_interval_ms, 500);
    }

    #[test]
    fn test_reconnect_config_defaults() {
        let config = ReconnectConfig::default();

        assert_eq!(config.cooldown_ms, 60_000);
        assert_eq!(config.max_attempts, 0);
        assert_eq!(config.zero_data_threshold, 5);
    }

    #[test]
    fn test_reconnect_config_builder() {
        let config = ReconnectConfig::new()
            .with_cooldown_ms(30_000)
            .with_max_attempts(10)
            .with_zero_data_threshold(3);

        assert_eq!(config.cooldown_ms, 30_000);
        assert_eq!(config.max_attempts, 10);
        assert_eq!(config.zero_data_threshold, 3);
    }

    #[test]
    fn test_modbus_channel_with_reconnect() {
        let reconnect = ReconnectConfig::new().with_cooldown_ms(10_000);
        let config = ModbusChannelConfig::tcp("127.0.0.1:502").with_reconnect(reconnect);

        let channel = ModbusChannel::new(config, 1);
        assert_eq!(channel.config.reconnect.cooldown_ms, 10_000);
    }

    #[test]
    fn test_tcp_config_connection_mode() {
        let config = ModbusChannelConfig::tcp("192.168.1.100:502");
        assert_eq!(config.connection_mode, ConnectionMode::Tcp);
        assert_eq!(config.address, "192.168.1.100:502");

        let channel = ModbusChannel::new(config, 1);
        assert_eq!(channel.name(), "Modbus TCP");
    }

    #[cfg(feature = "modbus")]
    #[test]
    fn test_rtu_config() {
        let config = ModbusChannelConfig::rtu("/dev/ttyUSB0", 9600);

        assert_eq!(config.connection_mode, ConnectionMode::Rtu);
        assert_eq!(config.rtu_device, "/dev/ttyUSB0");
        assert_eq!(config.baud_rate, 9600);

        let channel = ModbusChannel::new(config, 1);
        assert_eq!(channel.name(), "Modbus RTU");
    }
}
