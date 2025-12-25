//! GPIO (General Purpose Input/Output) protocol adapter.
//!
//! Provides direct hardware GPIO control on Linux systems with a pluggable driver architecture.
//!
//! # Platform Support
//!
//! **Linux only**: Supports multiple GPIO backends:
//! - `gpiod`: Modern character device interface (libgpiod v2) - **recommended**
//! - `sysfs`: Legacy sysfs interface (`/sys/class/gpio/`) - for compatibility
//!
//! # Feature Flag
//!
//! Requires `gpio` feature to be enabled.
//!
//! # Driver Architecture
//!
//! The GPIO module uses a trait-based driver system for extensibility:
//!
//! ```text
//!              ┌─────────────────────────┐
//!              │     GpioDriver trait    │
//!              └───────────┬─────────────┘
//!                          │
//!      ┌───────────┬───────┼───────┬───────────┐
//!      ▼           ▼       ▼       ▼           ▼
//!   GpiodDriver  SysfsDriver  (future)  MockDriver  ...
//!   (chardev)    (/sys/)               (testing)
//! ```
//!
//! # Example
//!
//! ```rust,ignore
//! use igw::protocols::gpio::{GpioChannel, GpioChannelConfig, GpioPinConfig, GpioDriverType};
//!
//! // Using gpiod (chardev) - recommended
//! let config = GpioChannelConfig::new()
//!     .with_driver(GpioDriverType::Gpiod)
//!     .add_pin(GpioPinConfig::digital_input("gpiochip0", 17, 1))
//!     .add_pin(GpioPinConfig::digital_output("gpiochip0", 18, 101));
//!
//! // Using sysfs - for legacy compatibility
//! let config = GpioChannelConfig::new()
//!     .with_driver(GpioDriverType::Sysfs { base_path: "/sys/class/gpio".into() })
//!     .add_pin(GpioPinConfig::digital_input_sysfs(490, 1))  // GPIO 490
//!     .add_pin(GpioPinConfig::digital_output_sysfs(491, 101));
//!
//! let mut gpio = GpioChannel::new(config);
//! gpio.connect().await?;
//!
//! // Read DI
//! let response = gpio.read(ReadRequest::all()).await?;
//!
//! // Control DO
//! gpio.write_control(&[ControlCommand::latching(101, true)]).await?;
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tokio::sync::RwLock;

use tokio_gpiod::{Chip, Options};

use crate::core::data::{DataBatch, DataPoint};
use crate::core::error::{GatewayError, Result};
use crate::core::logging::{ChannelLogConfig, ChannelLogHandler, LogContext, LoggableProtocol};
use crate::core::metadata::{DriverMetadata, HasMetadata, ParameterMetadata, ParameterType};
use crate::core::traits::{
    AdjustmentCommand, CommunicationMode, ConnectionState, ControlCommand, Diagnostics,
    PollingConfig, Protocol, ProtocolCapabilities, ProtocolClient, ReadRequest, ReadResponse,
    WriteResult,
};

// ============================================================================
// GPIO Driver Trait (Extensible Driver Architecture)
// ============================================================================

/// GPIO driver type selection.
///
/// Determines which backend is used for GPIO operations.
#[derive(Debug, Clone)]
pub enum GpioDriverType {
    /// Modern character device interface (libgpiod v2).
    /// Uses `/dev/gpiochipN` devices. **Recommended for new projects.**
    Gpiod,

    /// Legacy sysfs interface.
    /// Uses `/sys/class/gpio/` filesystem. For compatibility with older systems.
    Sysfs {
        /// Base path for sysfs GPIO (default: "/sys/class/gpio")
        base_path: String,
    },
}

impl Default for GpioDriverType {
    fn default() -> Self {
        Self::Gpiod
    }
}

/// GPIO driver trait - extensible interface for GPIO backends.
///
/// Implement this trait to add support for new GPIO backends (e.g., BDaq, custom hardware).
///
/// # Example
///
/// ```rust,ignore
/// pub struct MyCustomDriver { /* ... */ }
///
/// #[async_trait]
/// impl GpioDriver for MyCustomDriver {
///     fn name(&self) -> &'static str { "my-custom" }
///     async fn read_pin(&self, gpio_num: u32) -> Result<bool> { /* ... */ }
///     async fn write_pin(&self, gpio_num: u32, value: bool) -> Result<()> { /* ... */ }
/// }
/// ```
#[async_trait]
pub trait GpioDriver: Send + Sync {
    /// Driver name for diagnostics.
    fn name(&self) -> &'static str;

    /// Read a GPIO pin value.
    ///
    /// # Arguments
    /// * `pin` - Pin configuration (contains chip/gpio_number, direction, etc.)
    ///
    /// # Returns
    /// Raw pin value (before active_low adjustment).
    async fn read_pin(&self, pin: &GpioPinConfig) -> Result<bool>;

    /// Write a GPIO pin value.
    ///
    /// # Arguments
    /// * `pin` - Pin configuration
    /// * `value` - Value to write (before active_low adjustment)
    async fn write_pin(&self, pin: &GpioPinConfig, value: bool) -> Result<()>;

    /// Initialize the driver (optional).
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    /// Shutdown the driver (optional).
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

// ============================================================================
// Gpiod Driver (Modern chardev interface)
// ============================================================================

/// Gpiod driver using libgpiod v2 character device interface.
///
/// Uses `/dev/gpiochipN` for GPIO access. This is the recommended driver for modern Linux systems.
pub struct GpiodDriver;

impl GpiodDriver {
    /// Create a new gpiod driver.
    pub fn new() -> Self {
        Self
    }
}

impl HasMetadata for GpiodDriver {
    fn metadata() -> DriverMetadata {
        DriverMetadata {
            name: "gpiod",
            display_name: "Gpiod (Recommended)",
            description: "Modern character device interface using /dev/gpiochipN. Recommended for new projects.",
            is_recommended: true,
            example_config: serde_json::json!({
                "driver": "gpiod",
                "gpio_chip": "gpiochip6",
                "poll_interval_ms": 200,
                "pins": [
                    { "chip": "gpiochip6", "pin": 0, "direction": "input", "point_id": 1 },
                    { "chip": "gpiochip6", "pin": 1, "direction": "output", "point_id": 101 }
                ]
            }),
            parameters: vec![
                ParameterMetadata::optional(
                    "driver",
                    "Driver",
                    "GPIO driver type: 'gpiod' or 'sysfs'",
                    ParameterType::String,
                    serde_json::json!("gpiod"),
                ),
                ParameterMetadata::optional(
                    "gpio_chip",
                    "GPIO Chip",
                    "Default GPIO chip device name (e.g., gpiochip0, gpiochip6)",
                    ParameterType::String,
                    serde_json::json!("gpiochip0"),
                ),
                ParameterMetadata::optional(
                    "poll_interval_ms",
                    "Poll Interval (ms)",
                    "Polling interval for input pins in milliseconds",
                    ParameterType::Integer,
                    serde_json::json!(200),
                ),
                ParameterMetadata::required(
                    "pins",
                    "Pin Configuration",
                    "Array of GPIO pin configurations with chip, pin, direction, and point_id",
                    ParameterType::Array,
                ),
            ],
        }
    }
}

impl Default for GpiodDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl GpioDriver for GpiodDriver {
    fn name(&self) -> &'static str {
        "gpiod"
    }

    async fn read_pin(&self, pin: &GpioPinConfig) -> Result<bool> {
        let chip = Chip::new(&pin.chip).await.map_err(|e| {
            GatewayError::Protocol(format!("Failed to open GPIO chip '{}': {}", pin.chip, e))
        })?;

        let opts = Options::input([pin.pin]).consumer("igw");
        let lines = chip.request_lines(opts).await.map_err(|e| {
            GatewayError::Protocol(format!(
                "Failed to request GPIO line {} on chip '{}': {}",
                pin.pin, pin.chip, e
            ))
        })?;

        let values = lines.get_values([false]).await.map_err(|e| {
            GatewayError::Protocol(format!("Failed to read GPIO pin {}: {}", pin.pin, e))
        })?;

        Ok(values[0])
    }

    async fn write_pin(&self, pin: &GpioPinConfig, value: bool) -> Result<()> {
        let chip = Chip::new(&pin.chip).await.map_err(|e| {
            GatewayError::Protocol(format!("Failed to open GPIO chip '{}': {}", pin.chip, e))
        })?;

        let opts = Options::output([pin.pin]).consumer("igw").values([value]);
        let lines = chip.request_lines(opts).await.map_err(|e| {
            GatewayError::Protocol(format!(
                "Failed to request GPIO line {} on chip '{}': {}",
                pin.pin, pin.chip, e
            ))
        })?;

        lines.set_values([value]).await.map_err(|e| {
            GatewayError::Protocol(format!("Failed to write GPIO pin {}: {}", pin.pin, e))
        })?;

        Ok(())
    }
}

// ============================================================================
// Sysfs Driver (Legacy interface)
// ============================================================================

/// Sysfs driver using legacy `/sys/class/gpio/` interface.
///
/// This driver is provided for compatibility with:
/// - Older Linux kernels (< 4.8)
/// - Industrial devices that use sysfs (e.g., Advantech ECU series)
/// - Systems where GPIO is already exported via sysfs
///
/// **Note**: sysfs GPIO is deprecated since Linux 4.8. Use `GpiodDriver` for new projects.
pub struct SysfsDriver {
    base_path: String,
}

impl SysfsDriver {
    /// Create a new sysfs driver.
    ///
    /// # Arguments
    /// * `base_path` - Path to sysfs GPIO (typically "/sys/class/gpio")
    pub fn new(base_path: impl Into<String>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    /// Get the path for a GPIO's direction file.
    fn direction_path(&self, gpio_num: u32) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.base_path)
            .join(format!("gpio{}", gpio_num))
            .join("direction")
    }

    /// Get the path for a GPIO's value file.
    fn value_path(&self, gpio_num: u32) -> std::path::PathBuf {
        std::path::PathBuf::from(&self.base_path)
            .join(format!("gpio{}", gpio_num))
            .join("value")
    }

    /// Export a GPIO if not already exported.
    async fn ensure_exported(&self, gpio_num: u32) -> Result<()> {
        let gpio_path = std::path::PathBuf::from(&self.base_path).join(format!("gpio{}", gpio_num));

        if !gpio_path.exists() {
            let export_path = std::path::PathBuf::from(&self.base_path).join("export");
            tokio::fs::write(&export_path, gpio_num.to_string())
                .await
                .map_err(|e| {
                    GatewayError::Protocol(format!("Failed to export GPIO {}: {}", gpio_num, e))
                })?;

            // Wait for sysfs to create the GPIO directory
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Set GPIO direction.
    async fn set_direction(&self, gpio_num: u32, direction: GpioDirection) -> Result<()> {
        let dir_str = match direction {
            GpioDirection::Input => "in",
            GpioDirection::Output => "out",
        };

        tokio::fs::write(self.direction_path(gpio_num), dir_str)
            .await
            .map_err(|e| {
                GatewayError::Protocol(format!(
                    "Failed to set GPIO {} direction to {}: {}",
                    gpio_num, dir_str, e
                ))
            })?;

        Ok(())
    }
}

impl HasMetadata for SysfsDriver {
    fn metadata() -> DriverMetadata {
        DriverMetadata {
            name: "sysfs",
            display_name: "Sysfs (Legacy)",
            description: "Legacy sysfs interface using /sys/class/gpio/. For compatibility with older systems.",
            is_recommended: false,
            example_config: serde_json::json!({
                "driver": "sysfs",
                "gpio_base_path": "/sys/class/gpio",
                "poll_interval_ms": 200,
                "pins": [
                    { "gpio_number": 490, "direction": "input", "point_id": 1 },
                    { "gpio_number": 491, "direction": "output", "point_id": 101 }
                ]
            }),
            parameters: vec![
                ParameterMetadata::optional(
                    "driver",
                    "Driver",
                    "GPIO driver type: 'gpiod' or 'sysfs'",
                    ParameterType::String,
                    serde_json::json!("sysfs"),
                ),
                ParameterMetadata::optional(
                    "gpio_base_path",
                    "GPIO Base Path",
                    "Base path for sysfs GPIO interface",
                    ParameterType::String,
                    serde_json::json!("/sys/class/gpio"),
                ),
                ParameterMetadata::optional(
                    "poll_interval_ms",
                    "Poll Interval (ms)",
                    "Polling interval for input pins in milliseconds",
                    ParameterType::Integer,
                    serde_json::json!(200),
                ),
                ParameterMetadata::required(
                    "pins",
                    "Pin Configuration",
                    "Array of GPIO pin configurations with gpio_number, direction, and point_id",
                    ParameterType::Array,
                ),
            ],
        }
    }
}

impl Default for SysfsDriver {
    fn default() -> Self {
        Self::new("/sys/class/gpio")
    }
}

#[async_trait]
impl GpioDriver for SysfsDriver {
    fn name(&self) -> &'static str {
        "sysfs"
    }

    async fn read_pin(&self, pin: &GpioPinConfig) -> Result<bool> {
        let gpio_num = pin.gpio_number.ok_or_else(|| {
            GatewayError::Protocol(format!(
                "GPIO number not set for pin {} (required for sysfs driver)",
                pin.point_id
            ))
        })?;

        // Ensure GPIO is exported and set as input
        self.ensure_exported(gpio_num).await?;
        self.set_direction(gpio_num, GpioDirection::Input).await?;

        // Read value
        let value_str = tokio::fs::read_to_string(self.value_path(gpio_num))
            .await
            .map_err(|e| {
                GatewayError::Protocol(format!("Failed to read GPIO {}: {}", gpio_num, e))
            })?;

        let value = value_str.trim() == "1";
        Ok(value)
    }

    async fn write_pin(&self, pin: &GpioPinConfig, value: bool) -> Result<()> {
        let gpio_num = pin.gpio_number.ok_or_else(|| {
            GatewayError::Protocol(format!(
                "GPIO number not set for pin {} (required for sysfs driver)",
                pin.point_id
            ))
        })?;

        // Ensure GPIO is exported and set as output
        self.ensure_exported(gpio_num).await?;
        self.set_direction(gpio_num, GpioDirection::Output).await?;

        // Write value
        let value_str = if value { "1" } else { "0" };
        tokio::fs::write(self.value_path(gpio_num), value_str)
            .await
            .map_err(|e| {
                GatewayError::Protocol(format!("Failed to write GPIO {}: {}", gpio_num, e))
            })?;

        Ok(())
    }
}

// ============================================================================
// Pin Configuration
// ============================================================================

/// GPIO pin direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpioDirection {
    /// Input pin (DI - Digital Input).
    Input,
    /// Output pin (DO - Digital Output).
    Output,
}

/// GPIO pin configuration.
///
/// Supports both gpiod (chip + pin) and sysfs (gpio_number) addressing.
#[derive(Debug, Clone)]
pub struct GpioPinConfig {
    /// GPIO chip name (e.g., "gpiochip0") - for gpiod driver.
    pub chip: String,

    /// Pin number/offset on the GPIO chip - for gpiod driver.
    pub pin: u32,

    /// Global GPIO number (e.g., 490) - for sysfs driver.
    /// This is the number used in `/sys/class/gpio/gpioN/`.
    pub gpio_number: Option<u32>,

    /// Pin direction.
    pub direction: GpioDirection,

    /// Point ID for SCADA mapping (matches DataPoint/ControlCommand IDs).
    pub point_id: u32,

    /// Active low (invert logic).
    pub active_low: bool,

    /// Debounce time for inputs (microseconds).
    pub debounce_us: Option<u64>,
}

impl GpioPinConfig {
    /// Create a digital input configuration for gpiod driver.
    ///
    /// # Arguments
    /// * `chip` - GPIO chip name (e.g., "gpiochip0")
    /// * `pin` - Pin offset on the chip
    /// * `point_id` - SCADA point ID
    pub fn digital_input(chip: impl Into<String>, pin: u32, point_id: u32) -> Self {
        Self {
            chip: chip.into(),
            pin,
            gpio_number: None,
            direction: GpioDirection::Input,
            point_id,
            active_low: false,
            debounce_us: Some(1000), // 1ms default debounce
        }
    }

    /// Create a digital output configuration for gpiod driver.
    pub fn digital_output(chip: impl Into<String>, pin: u32, point_id: u32) -> Self {
        Self {
            chip: chip.into(),
            pin,
            gpio_number: None,
            direction: GpioDirection::Output,
            point_id,
            active_low: false,
            debounce_us: None,
        }
    }

    /// Create a digital input configuration for sysfs driver.
    ///
    /// # Arguments
    /// * `gpio_number` - Global GPIO number (e.g., 490 for `/sys/class/gpio/gpio490/`)
    /// * `point_id` - SCADA point ID
    pub fn digital_input_sysfs(gpio_number: u32, point_id: u32) -> Self {
        Self {
            chip: String::new(),
            pin: 0,
            gpio_number: Some(gpio_number),
            direction: GpioDirection::Input,
            point_id,
            active_low: false,
            debounce_us: Some(1000),
        }
    }

    /// Create a digital output configuration for sysfs driver.
    pub fn digital_output_sysfs(gpio_number: u32, point_id: u32) -> Self {
        Self {
            chip: String::new(),
            pin: 0,
            gpio_number: Some(gpio_number),
            direction: GpioDirection::Output,
            point_id,
            active_low: false,
            debounce_us: None,
        }
    }

    /// Set GPIO number (for sysfs driver or dual-mode configuration).
    pub fn with_gpio_number(mut self, gpio_number: u32) -> Self {
        self.gpio_number = Some(gpio_number);
        self
    }

    /// Set active low mode.
    pub fn with_active_low(mut self, active_low: bool) -> Self {
        self.active_low = active_low;
        self
    }

    /// Set debounce time.
    pub fn with_debounce(mut self, debounce_us: u64) -> Self {
        self.debounce_us = Some(debounce_us);
        self
    }
}

/// GPIO channel configuration.
#[derive(Debug, Clone)]
pub struct GpioChannelConfig {
    /// Driver type selection.
    pub driver: GpioDriverType,

    /// Pin configurations.
    pub pins: Vec<GpioPinConfig>,

    /// Polling interval for inputs.
    pub poll_interval: Duration,
}

impl Default for GpioChannelConfig {
    fn default() -> Self {
        Self {
            driver: GpioDriverType::default(),
            pins: Vec::new(),
            poll_interval: Duration::from_millis(100),
        }
    }
}

impl GpioChannelConfig {
    /// Create a new configuration with default gpiod driver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new configuration with sysfs driver.
    pub fn new_sysfs(base_path: impl Into<String>) -> Self {
        Self {
            driver: GpioDriverType::Sysfs {
                base_path: base_path.into(),
            },
            pins: Vec::new(),
            poll_interval: Duration::from_millis(100),
        }
    }

    /// Set the driver type.
    pub fn with_driver(mut self, driver: GpioDriverType) -> Self {
        self.driver = driver;
        self
    }

    /// Add a pin configuration.
    pub fn add_pin(mut self, pin: GpioPinConfig) -> Self {
        self.pins.push(pin);
        self
    }

    /// Set poll interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Get input pins.
    pub fn input_pins(&self) -> impl Iterator<Item = &GpioPinConfig> {
        self.pins
            .iter()
            .filter(|p| p.direction == GpioDirection::Input)
    }

    /// Get output pins.
    pub fn output_pins(&self) -> impl Iterator<Item = &GpioPinConfig> {
        self.pins
            .iter()
            .filter(|p| p.direction == GpioDirection::Output)
    }
}

/// GPIO channel diagnostics.
#[derive(Debug, Default)]
struct GpioDiagnostics {
    read_count: u64,
    write_count: u64,
    error_count: u64,
    last_error: Option<String>,
}

/// GPIO channel adapter.
///
/// Provides digital input/output control via pluggable GPIO drivers.
/// Supports both modern gpiod (chardev) and legacy sysfs backends.
///
/// The service layer (comsrv) is responsible for persistence.
pub struct GpioChannel {
    config: GpioChannelConfig,
    /// Pluggable GPIO driver (trait object for extensibility)
    driver: Box<dyn GpioDriver>,
    state: Arc<std::sync::RwLock<ConnectionState>>,
    diagnostics: Arc<RwLock<GpioDiagnostics>>,
    poll_task: Option<tokio::task::JoinHandle<()>>,
    /// Output states cache (for read-back)
    output_states: Arc<RwLock<std::collections::HashMap<u32, bool>>>,
    /// Logging context
    log_ctx: LogContext,
}

impl GpioChannel {
    /// Create a new GPIO channel with the configured driver.
    pub fn new(config: GpioChannelConfig) -> Self {
        // Create driver based on configuration
        let driver: Box<dyn GpioDriver> = match &config.driver {
            GpioDriverType::Gpiod => Box::new(GpiodDriver::new()),
            GpioDriverType::Sysfs { base_path } => Box::new(SysfsDriver::new(base_path.clone())),
        };
        Self {
            config,
            driver,
            state: Arc::new(std::sync::RwLock::new(ConnectionState::Disconnected)),
            diagnostics: Arc::new(RwLock::new(GpioDiagnostics::default())),
            poll_task: None,
            output_states: Arc::new(RwLock::new(std::collections::HashMap::new())),
            log_ctx: LogContext::new(0), // channel_id set later
        }
    }

    /// Create a GPIO channel with a custom driver.
    ///
    /// This allows using custom driver implementations beyond the built-in ones.
    pub fn with_driver(config: GpioChannelConfig, driver: Box<dyn GpioDriver>) -> Self {
        Self {
            config,
            driver,
            state: Arc::new(std::sync::RwLock::new(ConnectionState::Disconnected)),
            diagnostics: Arc::new(RwLock::new(GpioDiagnostics::default())),
            poll_task: None,
            output_states: Arc::new(RwLock::new(std::collections::HashMap::new())),
            log_ctx: LogContext::new(0),
        }
    }

    /// Get the driver name.
    pub fn driver_name(&self) -> &'static str {
        self.driver.name()
    }

    /// Set the channel ID for logging (called by service layer).
    pub fn set_channel_id(&mut self, channel_id: u32) {
        self.log_ctx = LogContext::new(channel_id);
    }

    fn set_state(&self, state: ConnectionState) {
        if let Ok(mut s) = self.state.write() {
            *s = state;
        }
    }

    fn get_state(&self) -> ConnectionState {
        self.state
            .read()
            .map(|s| *s)
            .unwrap_or(ConnectionState::Error)
    }

    /// Read a single GPIO pin using the configured driver.
    async fn read_pin(&self, pin_config: &GpioPinConfig) -> Result<DataPoint> {
        let raw_value = self.driver.read_pin(pin_config).await?;
        let adjusted = if pin_config.active_low {
            !raw_value
        } else {
            raw_value
        };
        Ok(DataPoint::signal(pin_config.point_id, adjusted))
    }

    /// Write to a single GPIO pin using the configured driver.
    async fn write_pin(&self, pin_config: &GpioPinConfig, value: bool) -> Result<()> {
        let adjusted = if pin_config.active_low { !value } else { value };
        self.driver.write_pin(pin_config, adjusted).await?;

        // Update internal state cache for feedback
        self.output_states
            .write()
            .await
            .insert(pin_config.point_id, adjusted);

        Ok(())
    }

    /// Read output state (for feedback).
    async fn read_output_state(&self, point_id: u32) -> Option<bool> {
        self.output_states.read().await.get(&point_id).copied()
    }
}

impl ProtocolCapabilities for GpioChannel {
    fn name(&self) -> &'static str {
        "GPIO"
    }

    fn supported_modes(&self) -> &[CommunicationMode] {
        &[CommunicationMode::Polling]
    }
}

impl LoggableProtocol for GpioChannel {
    fn set_log_handler(&mut self, handler: Arc<dyn ChannelLogHandler>) {
        self.log_ctx.set_handler(handler);
    }

    fn set_log_config(&mut self, config: ChannelLogConfig) {
        self.log_ctx.set_config(config);
    }

    fn log_config(&self) -> &ChannelLogConfig {
        self.log_ctx.config()
    }
}

impl Protocol for GpioChannel {
    fn connection_state(&self) -> ConnectionState {
        self.get_state()
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse> {
        if !self.get_state().is_connected() {
            return Err(GatewayError::NotConnected);
        }

        let mut batch = DataBatch::new();

        // Read all input pins
        for pin in self.config.input_pins() {
            // Filter by request
            if let Some(ref ids) = request.point_ids {
                if !ids.contains(&pin.point_id) {
                    continue;
                }
            }

            match self.read_pin(pin).await {
                Ok(point) => batch.add(point),
                Err(e) => {
                    let mut diag = self.diagnostics.write().await;
                    diag.error_count += 1;
                    diag.last_error = Some(e.to_string());
                }
            }
        }

        // Also include output states as feedback
        for pin in self.config.output_pins() {
            if let Some(ref ids) = request.point_ids {
                if !ids.contains(&pin.point_id) {
                    continue;
                }
            }

            if let Some(state) = self.read_output_state(pin.point_id).await {
                batch.add(DataPoint::control(pin.point_id, state));
            }
        }

        {
            let mut diag = self.diagnostics.write().await;
            diag.read_count += 1;
        }

        // Return batch directly (service layer handles storage)
        Ok(ReadResponse::success(batch))
    }

    async fn diagnostics(&self) -> Result<Diagnostics> {
        let diag = self.diagnostics.read().await;
        let input_count = self.config.input_pins().count();
        let output_count = self.config.output_pins().count();

        Ok(Diagnostics {
            protocol: self.name().to_string(),
            connection_state: self.get_state(),
            read_count: diag.read_count,
            write_count: diag.write_count,
            error_count: diag.error_count,
            last_error: diag.last_error.clone(),
            extra: serde_json::json!({
                "input_pins": input_count,
                "output_pins": output_count,
            }),
        })
    }
}

impl ProtocolClient for GpioChannel {
    async fn connect(&mut self) -> Result<()> {
        let start = Instant::now();
        // In a real implementation, validate GPIO chips and pins exist
        self.set_state(ConnectionState::Connected);
        self.log_ctx
            .log_connected("gpio", start.elapsed().as_millis() as u64)
            .await;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        if let Some(task) = self.poll_task.take() {
            task.abort();
        }
        self.set_state(ConnectionState::Disconnected);
        self.log_ctx.log_disconnected(None).await;
        Ok(())
    }

    async fn poll_once(&mut self) -> Result<DataBatch> {
        let start = Instant::now();
        let response = self.read(ReadRequest::all()).await?;
        let batch = response.data;

        // Log poll cycle
        self.log_ctx
            .log_poll_cycle(
                batch.clone(),
                start.elapsed().as_millis() as u64,
                batch.len(),
                0,
            )
            .await;

        Ok(batch)
    }

    async fn write_control(&mut self, commands: &[ControlCommand]) -> Result<WriteResult> {
        let start = Instant::now();

        if !self.get_state().is_connected() {
            return Err(GatewayError::NotConnected);
        }

        let mut success_count = 0;
        let mut failures = Vec::new();

        for cmd in commands {
            // Find corresponding output pin
            let pin = self
                .config
                .pins
                .iter()
                .find(|p| p.point_id == cmd.id && p.direction == GpioDirection::Output);

            match pin {
                Some(p) => match self.write_pin(p, cmd.value).await {
                    Ok(()) => success_count += 1,
                    Err(e) => failures.push((cmd.id, e.to_string())),
                },
                None => {
                    failures.push((cmd.id, "Output pin not found".into()));
                }
            }
        }

        {
            let mut diag = self.diagnostics.write().await;
            diag.write_count += success_count as u64;
        }

        let result = WriteResult {
            success_count,
            failures,
        };

        // Log control write
        self.log_ctx
            .log_control_write(
                commands.to_vec(),
                Ok(result.clone()),
                start.elapsed().as_millis() as u64,
            )
            .await;

        Ok(result)
    }

    async fn write_adjustment(
        &mut self,
        _adjustments: &[AdjustmentCommand],
    ) -> Result<WriteResult> {
        // GPIO doesn't support analog output
        Err(GatewayError::Unsupported(
            "GPIO does not support analog adjustment".into(),
        ))
    }

    async fn start_polling(&mut self, _config: PollingConfig) -> Result<()> {
        // Note: Polling is stub implementation.
        // In production, the service layer (comsrv) handles the polling loop
        // and is responsible for persisting data from read() calls.
        Ok(())
    }

    async fn stop_polling(&mut self) -> Result<()> {
        if let Some(task) = self.poll_task.take() {
            task.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gpio_channel_connect() {
        let config = GpioChannelConfig::new()
            .add_pin(GpioPinConfig::digital_input("gpiochip0", 17, 1))
            .add_pin(GpioPinConfig::digital_output("gpiochip0", 18, 101));

        let mut gpio = GpioChannel::new(config);

        assert_eq!(gpio.connection_state(), ConnectionState::Disconnected);

        gpio.connect().await.unwrap();
        assert_eq!(gpio.connection_state(), ConnectionState::Connected);

        gpio.disconnect().await.unwrap();
        assert_eq!(gpio.connection_state(), ConnectionState::Disconnected);
    }

    #[tokio::test]
    async fn test_gpio_write_control() {
        let config =
            GpioChannelConfig::new().add_pin(GpioPinConfig::digital_output("gpiochip0", 18, 101));

        let mut gpio = GpioChannel::new(config);
        gpio.connect().await.unwrap();

        let result = gpio
            .write_control(&[ControlCommand::latching(101, true)])
            .await
            .unwrap();

        assert_eq!(result.success_count, 1);
        assert!(result.failures.is_empty());

        // Check output state
        let state = gpio.read_output_state(101).await;
        assert_eq!(state, Some(true));
    }

    #[tokio::test]
    async fn test_gpio_diagnostics() {
        let config = GpioChannelConfig::new()
            .add_pin(GpioPinConfig::digital_input("gpiochip0", 17, 1))
            .add_pin(GpioPinConfig::digital_output("gpiochip0", 18, 101));

        let gpio = GpioChannel::new(config);
        let diag = gpio.diagnostics().await.unwrap();

        assert_eq!(diag.protocol, "GPIO");
        assert_eq!(diag.extra["input_pins"], 1);
        assert_eq!(diag.extra["output_pins"], 1);
    }
}
