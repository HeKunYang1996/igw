# Industrial Gateway (igw)

A universal SCADA protocol library for Rust, providing unified abstractions for industrial communication protocols.

## Features

- **Protocol Agnostic**: Unified four-remote (T/S/C/A) data model
- **Dual Mode Support**: Polling and event-driven communication
- **Zero Business Coupling**: Pure protocol layer, no business logic dependencies
- **Native Async Traits**: Uses Rust 1.92+ RPITIT (no async_trait macro)
- **Modular Design**: Protocol implementations via feature flags

## Supported Protocols

| Protocol | Feature | Status |
|----------|---------|--------|
| Modbus TCP/RTU | `modbus` | Available |
| IEC 60870-5-104 | `iec104` | Available |
| OPC UA | `opcua` | Available |
| J1939/CAN | `j1939` | Available (Linux) |
| GPIO | `gpio` | Available (Linux) |
| Virtual Channel | `virtual-channel` | Available |
| DNP3 | - | Planned |

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
igw = "0.2"                                    # Core traits and data model
# igw = { version = "0.2", features = ["modbus"] }  # With Modbus adapter
# igw = { version = "0.2", features = ["full"] }    # All protocols
```

**Minimum Rust Version**: 1.85+

### Features

| Feature | Description |
|---------|-------------|
| `modbus` | Modbus TCP/RTU adapter |
| `iec104` | IEC 60870-5-104 adapter |
| `opcua` | OPC UA client adapter |
| `j1939` | J1939/CAN bus (Linux only) |
| `gpio` | GPIO DI/DO (Linux only) |
| `virtual-channel` | Virtual data channel |
| `serial` | Serial port support |
| `tracing-support` | Tracing integration |
| `full` | All features |

## Quick Start

### Basic Data Model

```rust
use igw::prelude::*;

fn main() {
    // Create data points using the four-remote model
    let temp = DataPoint::telemetry("temperature", 25.5);
    let door = DataPoint::signal("door_open", true);

    // Batch multiple points
    let mut batch = DataBatch::default();
    batch.add(temp);
    batch.add(door);

    println!("Batch contains {} points", batch.points.len());
}
```

### With Modbus Adapter

```rust
use igw::prelude::*;
use igw::protocols::modbus::{ModbusChannel, ModbusChannelConfig};

#[tokio::main]
async fn main() -> igw::Result<()> {
    // Configure Modbus TCP connection
    let config = ModbusChannelConfig::tcp("192.168.1.100:502")
        .with_timeout(Duration::from_secs(5));

    // Create channel with unique channel_id
    let mut channel = ModbusChannel::new(config, 1);

    // Connect to device
    channel.connect().await?;

    // Execute a single poll cycle
    let batch = channel.poll_once().await?;
    println!("Read {} points", batch.points.len());

    // Write control command
    let result = channel.write_control(&[
        ControlCommand::latching("pump", true)
    ]).await?;
    println!("Success: {}", result.success_count);

    channel.disconnect().await?;
    Ok(())
}
```

### With IEC 104 (Event-Driven)

```rust
use igw::prelude::*;
use igw::protocols::iec104::{Iec104Channel, Iec104ChannelConfig};

#[tokio::main]
async fn main() -> igw::Result<()> {
    let config = Iec104ChannelConfig::new("192.168.1.100:2404");
    let mut channel = Iec104Channel::new(config);

    channel.connect().await?;

    // Subscribe to data events
    let mut rx = channel.subscribe();

    // Start receiving spontaneous data
    channel.start_polling(PollingConfig::default()).await?;

    while let Some(event) = rx.recv().await {
        match event {
            DataEvent::PointUpdate(point) => {
                println!("Point {}: {:?}", point.id, point.value);
            }
            DataEvent::BatchUpdate(batch) => {
                println!("Received {} points", batch.points.len());
            }
            _ => {}
        }
    }

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
    fn read(&self, request: ReadRequest) -> impl Future<Output = Result<ReadResponse>> + Send;
    fn diagnostics(&self) -> impl Future<Output = Result<Diagnostics>> + Send;
}
```

### `ProtocolClient` (Active Connection)

```rust
pub trait ProtocolClient: Protocol {
    fn connect(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn disconnect(&mut self) -> impl Future<Output = Result<()>> + Send;
    fn poll_once(&mut self) -> impl Future<Output = Result<DataBatch>> + Send;
    fn write_control(&mut self, commands: &[ControlCommand]) -> impl Future<Output = Result<WriteResult>> + Send;
    fn write_adjustment(&mut self, adjustments: &[AdjustmentCommand]) -> impl Future<Output = Result<WriteResult>> + Send;
    fn start_polling(&mut self, config: PollingConfig) -> impl Future<Output = Result<()>> + Send;
    fn stop_polling(&mut self) -> impl Future<Output = Result<()>> + Send;
}
```

### `EventDrivenProtocol` (For IEC 104, OPC UA)

```rust
pub trait EventDrivenProtocol: Protocol {
    fn subscribe(&self) -> DataEventReceiver;
    fn set_event_handler(&mut self, handler: Arc<dyn DataEventHandler>);
}
```

## Channel Logging

All protocol adapters support unified channel logging via the `LoggableProtocol` trait:

```rust
use std::sync::Arc;
use igw::{ChannelLogConfig, ChannelLogHandler, ChannelLogEvent, LoggableProtocol};
use igw::protocols::modbus::{ModbusChannel, ModbusChannelConfig};

// Implement custom log handler
struct MyLogHandler;

#[async_trait::async_trait]
impl ChannelLogHandler for MyLogHandler {
    async fn on_log(&self, channel_id: u32, event: ChannelLogEvent) {
        match event {
            ChannelLogEvent::Connected { endpoint, duration_ms, .. } => {
                println!("[CH:{}] Connected to {} ({}ms)", channel_id, endpoint, duration_ms);
            }
            ChannelLogEvent::Error { error, .. } => {
                eprintln!("[CH:{}] Error: {}", channel_id, error);
            }
            _ => {}
        }
    }
}

// Configure logging
let mut channel = ModbusChannel::new(ModbusChannelConfig::tcp("127.0.0.1:502"), 1);
channel.set_log_handler(Arc::new(MyLogHandler));
channel.set_log_config(ChannelLogConfig::all());  // or errors_only(), disabled()
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
