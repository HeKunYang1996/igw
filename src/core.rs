//! Core abstractions for the Industrial Gateway.
//!
//! This module provides the foundational types and traits that all protocols implement.

pub mod data;
pub mod error;
pub mod logging;
pub mod point;
pub mod quality;
pub mod traits;

pub use data::*;
pub use error::{GatewayError, Result};
pub use point::*;
pub use quality::*;
pub use traits::*;
