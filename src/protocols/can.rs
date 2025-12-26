//! CAN Protocol Implementation (LYNK Protocol)
//!
//! Implements CAN bus communication for Discover LYNK Serial CAN interface.
//! This implementation supports:
//! - Custom CSV-based point mapping configuration
//! - LYNK protocol frames (0x351-0x373)
//! - Little-Endian data decoding
//! - Event-driven data reception
//!
//! ## Features
//!
//! - **Event-Driven**: Passively listens to CAN bus, caches frames
//! - **CSV Configuration**: User-defined point mappings via CSV files
//! - **Flexible Decoding**: Supports various data types (uint8/16/32, int16/32, ASCII)
//!
//! ## Dependencies
//!
//! This module uses:
//! - [`socketcan`](https://crates.io/crates/socketcan) for CAN bus communication (Linux only)
//!
//! ## Example
//!
//! ```rust,ignore
//! use igw::protocols::can::{CanClient, CanConfig};
//!
//! let config = CanConfig {
//!     can_interface: "can0".to_string(),
//!     config_path: "/app/config/comsrv/1".into(),
//!     ..Default::default()
//! };
//!
//! let mut client = CanClient::new(config);
//! client.connect().await?;
//!
//! // Subscribe to data events
//! let mut rx = client.subscribe();
//! while let Some(event) = rx.recv().await {
//!     match event {
//!         DataEvent::DataUpdate(batch) => {
//!             println!("Received {} data points", batch.len());
//!         }
//!         _ => {}
//!     }
//! }
//! ```

mod client;
mod config;
mod decoder;

// Re-export client
pub use client::CanClient;
pub use config::{CanConfig, CanPoint, LynkCanId};

