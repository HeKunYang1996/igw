//! Protocol implementations.
//!
//! This module contains adapters that integrate protocol crates with igw.

pub mod virtual_channel;

// Modbus TCP + RTU support
#[cfg(feature = "modbus")]
#[cfg_attr(docsrs, doc(cfg(feature = "modbus")))]
pub mod modbus;

#[cfg(feature = "modbus")]
#[cfg_attr(docsrs, doc(cfg(feature = "modbus")))]
pub mod command_batcher;

#[cfg(feature = "iec104")]
#[cfg_attr(docsrs, doc(cfg(feature = "iec104")))]
pub mod iec104;

#[cfg(feature = "opcua")]
#[cfg_attr(docsrs, doc(cfg(feature = "opcua")))]
pub mod opcua;

#[cfg(all(feature = "can", target_os = "linux"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "can", target_os = "linux"))))]
pub mod can;

#[cfg(all(feature = "gpio", target_os = "linux"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "gpio", target_os = "linux"))))]
pub mod gpio;
