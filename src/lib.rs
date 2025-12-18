//! # Industrial Gateway (igw)
//!
//! A universal SCADA protocol library for Rust, providing unified abstractions
//! for industrial communication protocols.
//!
//! ## Features
//!
//! - **Protocol Agnostic**: Unified four-remote (T/S/C/A) data model
//! - **Dual Mode Support**: Polling and event-driven communication
//! - **Zero Business Coupling**: Pure protocol layer, no business logic
//! - **Feature Gated**: Compile only what you need
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use igw::prelude::*;
//! use voltage_modbus::ModbusTcpClient;  // Protocol from separate crate
//!
//! // Create a Modbus TCP client (implements igw::ProtocolClient)
//! let mut client = ModbusTcpClient::new("192.168.1.100:502")?;
//! client.connect().await?;
//!
//! // Read telemetry data
//! let response = client.read(ReadRequest::telemetry(vec![1, 2, 3])).await?;
//! ```
//!
//! ## Supported Protocols
//!
//! Protocol implementations are in separate crates:
//!
//! | Protocol | Crate | Status |
//! |----------|-------|--------|
//! | Modbus TCP/RTU | `voltage_modbus` | Available |
//! | IEC 60870-5-104 | `voltage_iec104` | Planned |
//! | DNP3 | `voltage_dnp3` | Planned |
//! | OPC UA | `voltage_opcua` | Planned |

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod core;
pub mod codec;
pub mod store;

#[cfg(feature = "modbus")]
#[cfg_attr(docsrs, doc(cfg(feature = "modbus")))]
pub mod protocols;

/// Prelude module for convenient imports
pub mod prelude {
    pub use crate::core::{
        traits::*,
        data::*,
        point::*,
        quality::*,
        error::{GatewayError, Result},
    };
    pub use crate::store::{DataStore, MemoryStore};
}

// Re-export core types at crate root for convenience
pub use crate::core::error::{GatewayError, Result};
pub use crate::core::data::{Value, DataType, DataPoint, DataBatch};
pub use crate::core::quality::Quality;
pub use crate::core::traits::{
    Protocol, ProtocolClient, ProtocolCapabilities,
    CommunicationMode, ConnectionState,
};

// Re-export store types
pub use crate::store::{DataStore, MemoryStore};
