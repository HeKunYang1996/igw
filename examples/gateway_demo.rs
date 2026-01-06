//! Gateway Demo - 演示如何使用 igw 构建网关运行时
//!
//! 这个示例展示了如何：
//! 1. 加载配置文件
//! 2. 使用 factory 创建协议通道
//! 3. 管理通道生命周期（connect/poll/disconnect）
//! 4. 处理数据事件和错误
//!
//! # 运行
//!
//! ```bash
//! cargo run --example gateway_demo --features full -- config.toml
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use serde::Serialize;
use tokio::sync::{broadcast, watch, Mutex};
use tokio::task::JoinHandle;

use igw::core::data::DataBatch;
use igw::core::error::Result;
use igw::core::traits::{DataEvent, Diagnostics, PointFailure};
use igw::gateway::{factory, ChannelRuntime, GatewayConfig};

// ============================================================================
// CLI
// ============================================================================

#[derive(Parser, Debug)]
#[command(name = "gateway_demo", about = "IGW Gateway Demo")]
struct Args {
    /// Configuration file path
    config: PathBuf,

    /// Output events as JSON Lines
    #[arg(long)]
    jsonl: bool,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

// ============================================================================
// Gateway Events
// ============================================================================

/// Gateway event types for unified event stream.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayEvent {
    Started {
        gateway_name: String,
        channel_count: usize,
    },
    Stopped {
        gateway_name: String,
    },
    ChannelConnected {
        channel_id: u32,
        channel_name: String,
        protocol: String,
    },
    ChannelDisconnected {
        channel_id: u32,
        channel_name: String,
        reason: Option<String>,
    },
    DataUpdate {
        channel_id: u32,
        batch: DataBatch,
    },
    PollResult {
        channel_id: u32,
        success_count: usize,
        failures: Vec<PointFailure>,
    },
    Error {
        channel_id: u32,
        error: String,
    },
    DiagnosticsSnapshot {
        channel_id: u32,
        diagnostics: DiagnosticsData,
    },
}

/// Serializable diagnostics data.
#[derive(Debug, Clone, Serialize)]
pub struct DiagnosticsData {
    pub protocol: String,
    pub read_count: u64,
    pub write_count: u64,
    pub error_count: u64,
    pub last_error: Option<String>,
}

impl From<Diagnostics> for DiagnosticsData {
    fn from(d: Diagnostics) -> Self {
        Self {
            protocol: d.protocol,
            read_count: d.read_count,
            write_count: d.write_count,
            error_count: d.error_count,
            last_error: d.last_error,
        }
    }
}

pub type GatewayEventSender = broadcast::Sender<GatewayEvent>;
pub type GatewayEventReceiver = broadcast::Receiver<GatewayEvent>;

// ============================================================================
// Gateway Runtime
// ============================================================================

/// Demo gateway runtime.
///
/// 这是一个示例实现，展示如何使用 igw 的 ChannelRuntime 构建网关。
/// 实际项目（如 comsrv）会有自己的调度逻辑、存储层、路由规则等。
pub struct Gateway {
    config: GatewayConfig,
    channels: Vec<Arc<Mutex<Box<dyn ChannelRuntime>>>>,
    event_tx: GatewayEventSender,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl Gateway {
    /// Create a gateway from configuration.
    pub fn from_config(config: GatewayConfig) -> Result<Self> {
        let (event_tx, _) = broadcast::channel(1024);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mut channels = Vec::new();

        for channel_config in config.enabled_channels() {
            match factory::create_channel(channel_config) {
                Ok(channel) => {
                    channels.push(Arc::new(Mutex::new(channel)));
                }
                Err(e) => {
                    eprintln!(
                        "Warning: Failed to create channel {} ({}): {}",
                        channel_config.id, channel_config.name, e
                    );
                }
            }
        }

        Ok(Self {
            config,
            channels,
            event_tx,
            shutdown_tx,
            shutdown_rx,
            tasks: Vec::new(),
        })
    }

    /// Subscribe to gateway events.
    pub fn subscribe(&self) -> GatewayEventReceiver {
        self.event_tx.subscribe()
    }

    /// Start the gateway.
    pub async fn start(&mut self) -> Result<()> {
        let _ = self.event_tx.send(GatewayEvent::Started {
            gateway_name: self.config.gateway.name.clone(),
            channel_count: self.channels.len(),
        });

        // Connect all channels
        for channel in &self.channels {
            let mut ch = channel.lock().await;
            let channel_id = ch.id();
            let channel_name = ch.name().to_string();
            let protocol = ch.protocol().to_string();

            match ch.connect().await {
                Ok(()) => {
                    let _ = self.event_tx.send(GatewayEvent::ChannelConnected {
                        channel_id,
                        channel_name,
                        protocol,
                    });
                }
                Err(e) => {
                    let _ = self.event_tx.send(GatewayEvent::Error {
                        channel_id,
                        error: e.to_string(),
                    });
                }
            }
        }

        // Start channel tasks
        for channel in &self.channels {
            let channel_id;
            let is_event_driven;
            let poll_interval;

            {
                let ch = channel.lock().await;
                channel_id = ch.id();
                is_event_driven = ch.is_event_driven();
                poll_interval = self
                    .config
                    .channels
                    .iter()
                    .find(|c| c.id == channel_id)
                    .and_then(|c| c.poll_interval_ms)
                    .unwrap_or(self.config.gateway.default_poll_interval_ms);
            }

            if is_event_driven {
                let task = self.spawn_event_task(Arc::clone(channel));
                self.tasks.push(task);
            } else {
                let task = self.spawn_polling_task(Arc::clone(channel), poll_interval);
                self.tasks.push(task);
            }
        }

        // Start diagnostics task
        let diag_task = self.spawn_diagnostics_task();
        self.tasks.push(diag_task);

        Ok(())
    }

    /// Stop the gateway gracefully.
    pub async fn stop(&mut self) -> Result<()> {
        let _ = self.shutdown_tx.send(true);

        for task in self.tasks.drain(..) {
            let _ = task.await;
        }

        for channel in &self.channels {
            let mut ch = channel.lock().await;
            let channel_id = ch.id();
            let channel_name = ch.name().to_string();

            if ch.is_event_driven() {
                let _ = ch.stop_events().await;
            }

            match ch.disconnect().await {
                Ok(()) => {
                    let _ = self.event_tx.send(GatewayEvent::ChannelDisconnected {
                        channel_id,
                        channel_name,
                        reason: None,
                    });
                }
                Err(e) => {
                    let _ = self.event_tx.send(GatewayEvent::ChannelDisconnected {
                        channel_id,
                        channel_name,
                        reason: Some(e.to_string()),
                    });
                }
            }
        }

        let _ = self.event_tx.send(GatewayEvent::Stopped {
            gateway_name: self.config.gateway.name.clone(),
        });

        Ok(())
    }

    fn spawn_polling_task(
        &self,
        channel: Arc<Mutex<Box<dyn ChannelRuntime>>>,
        poll_interval_ms: u64,
    ) -> JoinHandle<()> {
        let event_tx = self.event_tx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let interval = Duration::from_millis(poll_interval_ms);

        tokio::spawn(async move {
            let channel_id = channel.lock().await.id();

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = tokio::time::sleep(interval) => {
                        let result = {
                            let mut ch = channel.lock().await;
                            ch.poll_once().await
                        };

                        // Get count before moving data to avoid clone
                        let success_count = result.data.len();

                        if !result.data.is_empty() {
                            let _ = event_tx.send(GatewayEvent::DataUpdate {
                                channel_id,
                                batch: result.data, // move instead of clone
                            });
                        }

                        let _ = event_tx.send(GatewayEvent::PollResult {
                            channel_id,
                            success_count,
                            failures: result.failures,
                        });
                    }
                }
            }
        })
    }

    fn spawn_event_task(&self, channel: Arc<Mutex<Box<dyn ChannelRuntime>>>) -> JoinHandle<()> {
        let event_tx = self.event_tx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::spawn(async move {
            let channel_id;
            let mut data_rx;

            {
                let mut ch = channel.lock().await;
                channel_id = ch.id();

                if let Err(e) = ch.start_events().await {
                    let _ = event_tx.send(GatewayEvent::Error {
                        channel_id,
                        error: format!("Failed to start events: {}", e),
                    });
                    return;
                }

                match ch.subscribe() {
                    Some(rx) => data_rx = rx,
                    None => {
                        let _ = event_tx.send(GatewayEvent::Error {
                            channel_id,
                            error: "Channel does not support event subscription".to_string(),
                        });
                        return;
                    }
                }
            }

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    event = data_rx.recv() => {
                        match event {
                            Ok(DataEvent::DataUpdate(batch)) => {
                                let _ = event_tx.send(GatewayEvent::DataUpdate {
                                    channel_id,
                                    batch,
                                });
                            }
                            Ok(DataEvent::Error(e)) => {
                                let _ = event_tx.send(GatewayEvent::Error {
                                    channel_id,
                                    error: e,
                                });
                            }
                            Ok(DataEvent::ConnectionChanged(_)) | Ok(DataEvent::Heartbeat) => {}
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                eprintln!("Warning: Channel {} event receiver lagged by {}", channel_id, n);
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        })
    }

    fn spawn_diagnostics_task(&self) -> JoinHandle<()> {
        let channels = self.channels.clone();
        let event_tx = self.event_tx.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        let interval = Duration::from_millis(self.config.gateway.diagnostics_interval_ms);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => break,
                    _ = tokio::time::sleep(interval) => {
                        for channel in &channels {
                            let ch = channel.lock().await;
                            let channel_id = ch.id();

                            if let Ok(diag) = ch.diagnostics().await {
                                let _ = event_tx.send(GatewayEvent::DiagnosticsSnapshot {
                                    channel_id,
                                    diagnostics: diag.into(),
                                });
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub fn name(&self) -> &str {
        &self.config.gateway.name
    }
}

// ============================================================================
// Main
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    if std::env::var("RUST_LOG").is_err() {
        if args.verbose {
            std::env::set_var("RUST_LOG", "info,igw=debug");
        } else {
            std::env::set_var("RUST_LOG", "info");
        }
    }

    // Check config file
    if !args.config.exists() {
        eprintln!("Error: Config file not found: {:?}", args.config);
        std::process::exit(1);
    }

    // Load configuration
    let config_str = std::fs::read_to_string(&args.config)
        .map_err(|e| igw::core::error::GatewayError::Config(e.to_string()))?;
    let config: GatewayConfig = toml::from_str(&config_str)
        .map_err(|e| igw::core::error::GatewayError::Config(e.to_string()))?;

    if args.verbose {
        eprintln!(
            "Loaded config: {} with {} channels",
            config.gateway.name,
            config.channels.len()
        );
    }

    // Create gateway
    let mut gateway = Gateway::from_config(config)?;

    if args.verbose {
        eprintln!(
            "Created gateway with {} active channels",
            gateway.channel_count()
        );
    }

    // Subscribe to events
    let mut events = gateway.subscribe();

    // Start gateway
    gateway.start().await?;

    eprintln!("Gateway started. Press Ctrl+C to stop.");

    // Spawn event handler
    let jsonl = args.jsonl;
    let event_task = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    if jsonl {
                        if let Ok(json) = serde_json::to_string(&event) {
                            println!("{}", json);
                        }
                    } else {
                        print_event(&event);
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("Warning: Event receiver lagged by {} messages", n);
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    eprintln!("\nShutting down...");

    // Stop gateway
    gateway.stop().await?;

    // Cancel event task
    event_task.abort();
    let _ = event_task.await;

    eprintln!("Gateway stopped.");
    Ok(())
}

fn print_event(event: &GatewayEvent) {
    match event {
        GatewayEvent::Started {
            gateway_name,
            channel_count,
        } => {
            println!("[STARTED] {} with {} channels", gateway_name, channel_count);
        }
        GatewayEvent::Stopped { gateway_name } => {
            println!("[STOPPED] {}", gateway_name);
        }
        GatewayEvent::ChannelConnected {
            channel_id,
            channel_name,
            protocol,
        } => {
            println!(
                "[CONNECTED] Channel {} ({}) using {}",
                channel_id, channel_name, protocol
            );
        }
        GatewayEvent::ChannelDisconnected {
            channel_id,
            channel_name,
            reason,
        } => {
            let reason_str = reason
                .as_ref()
                .map(|r| format!(": {}", r))
                .unwrap_or_default();
            println!(
                "[DISCONNECTED] Channel {} ({}){}",
                channel_id, channel_name, reason_str
            );
        }
        GatewayEvent::DataUpdate { channel_id, batch } => {
            println!("[DATA] Channel {}: {} points", channel_id, batch.len());
            for point in batch.iter().take(5) {
                println!("  - Point {}: {:?}", point.id, point.value);
            }
            if batch.len() > 5 {
                println!("  ... and {} more", batch.len() - 5);
            }
        }
        GatewayEvent::PollResult {
            channel_id,
            success_count,
            failures,
        } => {
            if !failures.is_empty() {
                println!(
                    "[POLL] Channel {}: {} success, {} failures",
                    channel_id,
                    success_count,
                    failures.len()
                );
                for failure in failures.iter().take(3) {
                    println!("  - Point {}: {}", failure.point_id, failure.error);
                }
            }
        }
        GatewayEvent::Error { channel_id, error } => {
            eprintln!("[ERROR] Channel {}: {}", channel_id, error);
        }
        GatewayEvent::DiagnosticsSnapshot {
            channel_id,
            diagnostics,
        } => {
            println!(
                "[DIAG] Channel {} ({}): reads={}, writes={}, errors={}",
                channel_id,
                diagnostics.protocol,
                diagnostics.read_count,
                diagnostics.write_count,
                diagnostics.error_count
            );
        }
    }
}
