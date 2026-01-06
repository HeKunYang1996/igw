//! IGW CLI Entry Point
//!
//! 工具命令行界面，提供协议查询和示例配置生成。
//!
//! 要运行完整的网关，请使用 `examples/gateway_demo.rs`：
//! ```bash
//! cargo run --example gateway_demo --features full -- config.toml
//! ```

use clap::{Parser, Subcommand};

use igw::core::metadata::get_protocol_registry;

/// Industrial Gateway - Universal SCADA Protocol Gateway
#[derive(Parser, Debug)]
#[command(name = "igw", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List supported protocols
    ListProtocols,

    /// Generate example configuration
    Example {
        /// Protocol to generate example for
        #[arg(default_value = "modbus")]
        protocol: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::ListProtocols => {
            list_protocols();
        }
        Commands::Example { protocol } => {
            generate_example(&protocol);
        }
    }
}

fn list_protocols() {
    let registry = get_protocol_registry();

    println!("Supported protocols:");
    println!();

    for protocol in registry.protocols() {
        println!("  {} ({})", protocol.name, protocol.display_name);
        println!("    {}", protocol.description);
        if !protocol.drivers.is_empty() {
            println!("    Drivers:");
            for driver in &protocol.drivers {
                let rec = if driver.is_recommended {
                    " (recommended)"
                } else {
                    ""
                };
                println!("      - {}{}", driver.name, rec);
            }
        }
        println!();
    }

    println!("For a complete gateway demo, run:");
    println!("  cargo run --example gateway_demo --features full -- <config.toml>");
}

fn generate_example(protocol: &str) {
    // Use eq_ignore_ascii_case to avoid String allocation from to_lowercase()
    let example = if protocol.eq_ignore_ascii_case("modbus") {
        r#"# IGW Configuration - Modbus Example

[gateway]
name = "Modbus Gateway"
default_poll_interval_ms = 1000
diagnostics_interval_ms = 30000

[[channels]]
id = 1
name = "PLC1"
protocol = "modbus"
enabled = true

[channels.parameters]
host = "192.168.1.100"
port = 502
connect_timeout_ms = 5000
io_timeout_ms = 3000

[[channels.points]]
id = 1001
name = "Temperature"
address = "1:100"  # slave_id:register

[channels.points.transform]
scale = 0.1

[[channels.points]]
id = 1002
name = "Pressure"
address = "1:101"

[channels.points.transform]
scale = 0.01
offset = 0.0
"#
    } else if protocol.eq_ignore_ascii_case("iec104") {
        r#"# IGW Configuration - IEC 104 Example

[gateway]
name = "IEC104 Gateway"
default_poll_interval_ms = 1000
diagnostics_interval_ms = 30000

[[channels]]
id = 1
name = "RTU1"
protocol = "iec104"
enabled = true

[channels.parameters]
address = "192.168.1.200:2404"
common_address = 1
connect_timeout_ms = 10000

[[channels.points]]
id = 2001
name = "Voltage_A"
address = "1001"  # IOA

[[channels.points]]
id = 2002
name = "Current_A"
address = "1002"
"#
    } else if protocol.eq_ignore_ascii_case("virtual") {
        r#"# IGW Configuration - Virtual Channel Example

[gateway]
name = "Virtual Gateway"
default_poll_interval_ms = 1000
diagnostics_interval_ms = 30000

[[channels]]
id = 1
name = "DataHub"
protocol = "virtual"
enabled = true

[channels.parameters]
name = "data_hub"
buffer_size = 2048

[[channels.points]]
id = 1001
name = "SimulatedTemp"
address = "temperature"

[[channels.points]]
id = 1002
name = "SimulatedPressure"
address = "pressure"
"#
    } else {
        eprintln!("Unknown protocol: {}", protocol);
        eprintln!("Available: modbus, iec104, virtual");
        return;
    };

    println!("{}", example);
}
