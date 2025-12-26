//! CAN protocol configuration types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// CAN client configuration.
#[derive(Debug, Clone)]
pub struct CanConfig {
    /// CAN interface name (e.g., "can0").
    pub can_interface: String,

    /// CAN bitrate (bits per second).
    pub bitrate: u32,

    /// RX polling interval in milliseconds.
    pub rx_poll_interval_ms: u64,

    /// Data reading interval in milliseconds.
    pub data_read_interval_ms: u64,
}

impl Default for CanConfig {
    fn default() -> Self {
        Self {
            can_interface: "can0".to_string(),
            bitrate: 250000,
            rx_poll_interval_ms: 50,
            data_read_interval_ms: 1000,
        }
    }
}

/// CAN point mapping structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanPoint {
    /// Unique point identifier (numeric)
    pub point_id: u32,
    /// CAN-ID (e.g., 0x351)
    pub can_id: u32,
    /// Byte offset in CAN data field (0-7)
    pub byte_offset: u8,
    /// Bit starting position within byte (0-7, LSB=0)
    pub bit_position: u8,
    /// Bit length (2/8/16/32/64)
    pub bit_length: u8,
    /// Data type (uint8, uint16, int16, uint32, int32, ascii)
    pub data_type: String,
    /// Scale factor for linear transformation (value = raw * scale + offset)
    #[serde(default = "default_scale")]
    pub scale: f64,
    /// Offset for linear transformation
    #[serde(default)]
    pub offset: f64,
}

fn default_scale() -> f64 {
    1.0
}

/// CAN frame cache - stores the latest received frame for each CAN-ID
#[derive(Debug, Clone, Default)]
pub struct CanFrameCache {
    /// Map from CAN-ID to frame data (up to 8 bytes)
    frames: HashMap<u32, Vec<u8>>,
}

impl CanFrameCache {
    /// Create a new empty frame cache
    pub fn new() -> Self {
        Self {
            frames: HashMap::new(),
        }
    }

    /// Update cache with a new frame
    pub fn update(&mut self, can_id: u32, data: Vec<u8>) {
        self.frames.insert(can_id, data);
    }

    /// Get the latest frame data for a CAN-ID
    pub fn get(&self, can_id: u32) -> Option<&[u8]> {
        self.frames.get(&can_id).map(|v| v.as_slice())
    }

    /// Check if a CAN-ID has been received
    pub fn contains(&self, can_id: u32) -> bool {
        self.frames.contains_key(&can_id)
    }

    /// Get all cached CAN-IDs
    pub fn can_ids(&self) -> Vec<u32> {
        self.frames.keys().copied().collect()
    }

    /// Get number of cached CAN-IDs
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Get all frames (for debugging)
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &Vec<u8>)> {
        self.frames.iter()
    }
}

/// LYNK Serial CAN protocol CAN-IDs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum LynkCanId {
    /// Battery Limits (1s period)
    BatteryLimits = 0x351,
    /// Battery Capacity Information (1s period)
    BatteryCapacity = 0x354,
    /// Battery Status (SOC/SOH) (1s period)
    BatteryStatus = 0x355,
    /// Battery Measurements (voltage/current/temp) (1s period)
    BatteryMeasurements = 0x356,
    /// Battery Alarms & Warnings (1s period)
    BatteryAlarms = 0x35A,
    /// Manufacturer Name ASCII (10s period)
    ManufacturerName = 0x35E,
    /// Model Name Upper ASCII (10s period)
    ModelNameUpper = 0x370,
    /// Model Name Lower ASCII (10s period)
    ModelNameLower = 0x371,
    /// Firmware Version (10s period)
    FirmwareVersion = 0x372,
    /// Protocol Version (10s period)
    ProtocolVersion = 0x373,
}

impl LynkCanId {
    /// Convert to u32
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Try to create from u32
    pub fn from_u32(id: u32) -> Option<Self> {
        match id {
            0x351 => Some(Self::BatteryLimits),
            0x354 => Some(Self::BatteryCapacity),
            0x355 => Some(Self::BatteryStatus),
            0x356 => Some(Self::BatteryMeasurements),
            0x35A => Some(Self::BatteryAlarms),
            0x35E => Some(Self::ManufacturerName),
            0x370 => Some(Self::ModelNameUpper),
            0x371 => Some(Self::ModelNameLower),
            0x372 => Some(Self::FirmwareVersion),
            0x373 => Some(Self::ProtocolVersion),
            _ => None,
        }
    }

    /// Check if this is a LYNK protocol CAN-ID
    pub fn is_lynk_id(id: u32) -> bool {
        Self::from_u32(id).is_some()
    }

    /// Get description
    pub fn description(self) -> &'static str {
        match self {
            Self::BatteryLimits => "Battery Limits",
            Self::BatteryCapacity => "Battery Capacity Information",
            Self::BatteryStatus => "Battery Status",
            Self::BatteryMeasurements => "Battery Measurements",
            Self::BatteryAlarms => "Battery Alarms & Warnings",
            Self::ManufacturerName => "Manufacturer Name",
            Self::ModelNameUpper => "Model Name Upper",
            Self::ModelNameLower => "Model Name Lower",
            Self::FirmwareVersion => "Firmware Version",
            Self::ProtocolVersion => "Protocol Version",
        }
    }
}

