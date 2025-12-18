//! Data storage abstraction layer.
//!
//! This module provides the `DataStore` trait for abstracting data storage,
//! allowing the gateway to work with different backends:
//!
//! - `MemoryStore`: In-memory storage using DashMap (default, zero dependencies)
//! - `RedisStore`: Redis-based storage (optional, for VoltageEMS integration)
//!
//! # Example
//!
//! ```rust,ignore
//! use igw::store::{DataStore, MemoryStore};
//!
//! let store = MemoryStore::new();
//! // Use store with Gateway...
//! ```

mod memory;
mod traits;

pub use memory::MemoryStore;
pub use traits::DataStore;
