//! Protocol implementations.
//!
//! This module contains adapters that integrate protocol crates with igw.

#[cfg(feature = "modbus")]
#[cfg_attr(docsrs, doc(cfg(feature = "modbus")))]
pub mod modbus;
