//! CAN frame data decoder
//!
//! Provides functions to extract and decode fields from CAN frame data,
//! supporting various data types with Little-Endian byte ordering

use crate::core::data::{DataType, Value};
use crate::core::error::{GatewayError, Result};
use crate::core::quality::Quality;
use crate::protocols::can::config::CanPoint;

use std::collections::HashMap;

/// Point mapping manager
pub struct PointManager {
    /// All points indexed by point_id
    points: HashMap<u32, CanPoint>,
    /// Points grouped by CAN-ID for efficient lookup
    points_by_can_id: HashMap<u32, Vec<u32>>,
}

impl PointManager {
    /// Create a new empty point manager
    pub fn new() -> Self {
        Self {
            points: HashMap::new(),
            points_by_can_id: HashMap::new(),
        }
    }

    /// Add a point to the manager
    pub fn add_point(&mut self, point: CanPoint) {
        let can_id = point.can_id;
        let point_id = point.point_id;

        self.points.insert(point_id, point);

        self.points_by_can_id
            .entry(can_id)
            .or_insert_with(Vec::new)
            .push(point_id);
    }

    /// Apply mappings to decode CAN frames into data points
    pub fn apply_mappings(
        &self,
        frame_cache: &super::config::CanFrameCache,
    ) -> Result<HashMap<u32, (Value, DataType, Quality)>> {
        let mut result = HashMap::new();

        for (point_id, point) in &self.points {
            if let Some(frame_data) = frame_cache.get(point.can_id) {
                match decode_point(point, frame_data) {
                    Ok((value, data_type)) => {
                        result.insert(*point_id, (value, data_type, Quality::Good));
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing-support")]
                        tracing::warn!("Failed to decode point {}: {}", point_id, e);
                    }
                }
            }
        }

        Ok(result)
    }

    /// Get points for a specific CAN-ID
    pub fn get_points_for_can_id(&self, can_id: u32) -> Vec<&CanPoint> {
        self.points_by_can_id
            .get(&can_id)
            .map(|ids| ids.iter().filter_map(|id| self.points.get(id)).collect())
            .unwrap_or_default()
    }
}

/// Extract a field from CAN data
fn extract_field(
    data: &[u8],
    byte_offset: u8,
    bit_position: u8,
    bit_length: u8,
) -> Result<Vec<u8>> {
    let byte_offset = byte_offset as usize;

    // Validate parameters
    if byte_offset >= data.len() {
        return Err(GatewayError::Protocol(format!(
            "Byte offset {} out of range (data length: {})",
            byte_offset,
            data.len()
        )));
    }

    // Handle different bit lengths
    match bit_length {
        2 => {
            // 2-bit field (for alarm status)
            if bit_position > 6 {
                return Err(GatewayError::Protocol(format!(
                    "Invalid bit position {} for 2-bit field",
                    bit_position
                )));
            }
            let byte = data[byte_offset];
            let value = (byte >> bit_position) & 0x03;
            Ok(vec![value])
        }
        8 => {
            // Single byte
            if bit_position != 0 {
                return Err(GatewayError::Protocol(format!(
                    "8-bit field must start at bit position 0, got {}",
                    bit_position
                )));
            }
            Ok(vec![data[byte_offset]])
        }
        16 => {
            // 2 bytes (Little-Endian)
            if bit_position != 0 {
                return Err(GatewayError::Protocol(format!(
                    "16-bit field must start at bit position 0, got {}",
                    bit_position
                )));
            }
            if byte_offset + 2 > data.len() {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for 16-bit field at offset {}",
                    byte_offset
                )));
            }
            Ok(vec![data[byte_offset], data[byte_offset + 1]])
        }
        32 => {
            // 4 bytes (Little-Endian)
            if bit_position != 0 {
                return Err(GatewayError::Protocol(format!(
                    "32-bit field must start at bit position 0, got {}",
                    bit_position
                )));
            }
            if byte_offset + 4 > data.len() {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for 32-bit field at offset {}",
                    byte_offset
                )));
            }
            Ok(data[byte_offset..byte_offset + 4].to_vec())
        }
        64 => {
            // 8 bytes (for ASCII strings)
            if bit_position != 0 {
                return Err(GatewayError::Protocol(format!(
                    "64-bit field must start at bit position 0, got {}",
                    bit_position
                )));
            }
            if byte_offset + 8 > data.len() {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for 64-bit field at offset {}",
                    byte_offset
                )));
            }
            Ok(data[byte_offset..byte_offset + 8].to_vec())
        }
        _ => Err(GatewayError::Protocol(format!(
            "Unsupported bit length: {}",
            bit_length
        ))),
    }
}

/// Decode a CAN point
fn decode_point(point: &CanPoint, frame_data: &[u8]) -> Result<(Value, DataType)> {
    // Extract raw bytes
    let raw_bytes = extract_field(
        frame_data,
        point.byte_offset,
        point.bit_position,
        point.bit_length,
    )?;

    // Decode based on data type
    let (raw_value, data_type) = match point.data_type.as_str() {
        "uint8" => {
            if raw_bytes.is_empty() {
                return Err(GatewayError::Protocol("Empty data for uint8".to_string()));
            }
            (Value::Integer(raw_bytes[0] as i64), DataType::Telemetry)
        }
        "uint16" => {
            if raw_bytes.len() < 2 {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for uint16, got {}",
                    raw_bytes.len()
                )));
            }
            let raw = u16::from_le_bytes([raw_bytes[0], raw_bytes[1]]);
            (Value::Integer(raw as i64), DataType::Telemetry)
        }
        "int16" => {
            if raw_bytes.len() < 2 {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for int16, got {}",
                    raw_bytes.len()
                )));
            }
            let raw = i16::from_le_bytes([raw_bytes[0], raw_bytes[1]]);
            (Value::Integer(raw as i64), DataType::Telemetry)
        }
        "uint32" => {
            if raw_bytes.len() < 4 {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for uint32, got {}",
                    raw_bytes.len()
                )));
            }
            let raw = u32::from_le_bytes([
                raw_bytes[0],
                raw_bytes[1],
                raw_bytes[2],
                raw_bytes[3],
            ]);
            (Value::Integer(raw as i64), DataType::Telemetry)
        }
        "int32" => {
            if raw_bytes.len() < 4 {
                return Err(GatewayError::Protocol(format!(
                    "Not enough bytes for int32, got {}",
                    raw_bytes.len()
                )));
            }
            let raw = i32::from_le_bytes([
                raw_bytes[0],
                raw_bytes[1],
                raw_bytes[2],
                raw_bytes[3],
            ]);
            (Value::Integer(raw as i64), DataType::Telemetry)
        }
        "int" => {
            // Generic "int" type - infer size from bit_length
            // For 2-bit signal values (common for alarms), treat as uint8
            if raw_bytes.is_empty() {
                return Err(GatewayError::Protocol("Empty data for int".to_string()));
            }
            (Value::Integer(raw_bytes[0] as i64), DataType::Signal)
        }
        "ascii" => {
            // Decode ASCII string, stopping at first null byte
            let s = String::from_utf8_lossy(&raw_bytes);
            let trimmed = s.trim_end_matches('\0').to_string();
            (Value::String(trimmed), DataType::Telemetry)
        }
        _ => {
            return Err(GatewayError::Protocol(format!(
                "Unsupported data type: {}",
                point.data_type
            )));
        }
    };

    // Apply scale and offset for numeric values
    let final_value = match raw_value {
        Value::Integer(i) if point.scale != 1.0 || point.offset != 0.0 => {
            let scaled = (i as f64) * point.scale + point.offset;
            Value::Float(scaled)
        }
        other => other,
    };

    Ok((final_value, data_type))
}

