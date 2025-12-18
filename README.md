# Industrial Gateway (igw)

A universal SCADA protocol library for Rust, providing unified abstractions for industrial communication protocols.

## Features

- **Protocol Agnostic**: Unified four-remote (T/S/C/A) data model
- **Dual Mode Support**: Polling and event-driven communication
- **Zero Business Coupling**: Pure protocol layer, no business logic dependencies
- **DataStore Abstraction**: Pluggable storage (MemoryStore, RedisStore)
- **Modular Design**: Protocol implementations in separate crates (pluggable)

## Supported Protocols

Protocol implementations are in separate crates:

| Protocol | Crate | Status |
|----------|-------|--------|
| Modbus TCP/RTU | `voltage_modbus` | Available |
| IEC 60870-5-104 | `voltage_iec104` | Planned |
| DNP3 | `voltage_dnp3` | Planned |
| OPC UA | `voltage_opcua` | Planned |

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
igw = "0.1"                           # Core traits and data model
# igw = { version = "0.1", features = ["modbus"] }  # With Modbus adapter
```

### Features

| Feature | Description |
|---------|-------------|
| `modbus` | Modbus TCP adapter (requires `voltage_modbus`) |
| `serial` | Serial port support |
| `tracing-support` | Tracing integration |
| `full` | All features |

## Quick Start

### Using DataStore

```rust
use igw::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> igw::Result<()> {
    // Create an in-memory data store
    let store = Arc::new(MemoryStore::new());

    // Write some data
    let mut batch = DataBatch::default();
    batch.add(DataPoint::telemetry("temperature", 25.5));
    batch.add(DataPoint::signal("door_open", true));
    store.write_batch(1, &batch).await?;

    // Read data back
    let point = store.read_point(1, "temperature").await?;
    println!("Temperature: {:?}", point.map(|p| p.value));

    Ok(())
}
```

### With Modbus Adapter

```rust
use igw::prelude::*;
use igw::protocols::modbus::{ModbusChannel, ModbusChannelConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> igw::Result<()> {
    let store = Arc::new(MemoryStore::new());

    // Create Modbus channel
    let config = ModbusChannelConfig::tcp("192.168.1.100:502");
    let mut channel = ModbusChannel::new(config, store);

    // Connect to device
    channel.connect().await?;

    // Write control command
    let result = channel.write_control(&[
        ControlCommand { id: "pump".into(), value: true }
    ]).await?;

    println!("Success: {}", result.success_count);

    channel.disconnect().await?;
    Ok(())
}
```

## Data Model

The library uses the "Four Remotes" concept common in SCADA systems:

| Type | Code | Direction | Description |
|------|------|-----------|-------------|
| Telemetry | T | Input | Analog measurements |
| Signal | S | Input | Digital status |
| Control | C | Output | Digital commands |
| Adjustment | A | Output | Analog setpoints |

## Protocol Traits

### `Protocol` (Base)

```rust
pub trait Protocol: Send + Sync {
    fn connection_state(&self) -> ConnectionState;
    async fn read(&self, request: ReadRequest) -> Result<ReadResponse>;
    async fn diagnostics(&self) -> Result<Diagnostics>;
}
```

### `ProtocolClient` (Active Connection)

```rust
pub trait ProtocolClient: Protocol {
    async fn connect(&mut self) -> Result<()>;
    async fn disconnect(&mut self) -> Result<()>;
    async fn write_control(&mut self, commands: &[ControlCommand]) -> Result<WriteResult>;
    async fn write_adjustment(&mut self, adjustments: &[AdjustmentCommand]) -> Result<WriteResult>;
    async fn start_polling(&mut self, config: PollingConfig) -> Result<()>;
    async fn stop_polling(&mut self) -> Result<()>;
}
```

### `EventDrivenProtocol` (For IEC 104, OPC UA)

```rust
pub trait EventDrivenProtocol: Protocol {
    fn subscribe(&self) -> DataEventReceiver;
    fn set_event_handler(&mut self, handler: Arc<dyn DataEventHandler>);
}
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
