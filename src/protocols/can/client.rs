//! CAN Protocol Client Implementation
//!
//! Implements the igw Protocol traits for LYNK CAN communication.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use socketcan::{CanSocket, EmbeddedFrame, Frame, Socket, SocketOptions};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::core::data::{DataBatch, DataPoint, DataType, Value};
use crate::core::error::{GatewayError, Result};
use crate::core::quality::Quality;
use crate::core::traits::{
    AdjustmentCommand, CommunicationMode, ConnectionState, ControlCommand, DataEvent,
    DataEventHandler, DataEventReceiver, DataEventSender, Diagnostics, EventDrivenProtocol,
    PollingConfig, Protocol, ProtocolCapabilities, ProtocolClient, ReadRequest, ReadResponse,
    WriteResult,
};

use super::config::{CanConfig, CanFrameCache, LynkCanId};
use super::decoder::PointManager;

// ============================================================================
// CanClient
// ============================================================================

/// CAN protocol client.
///
/// Implements event-driven communication over CAN bus using the LYNK protocol.
/// Uses CSV configuration for flexible point mapping.
pub struct CanClient {
    config: CanConfig,

    // Connection state
    connection_state: Arc<RwLock<ConnectionState>>,
    is_connected: Arc<AtomicBool>,

    // Statistics
    read_count: AtomicU64,
    error_count: AtomicU64,
    last_error: Arc<RwLock<Option<String>>>,

    // Tasks
    receive_handle: Option<JoinHandle<()>>,
    read_handle: Option<JoinHandle<()>>,

    // Event channel
    event_sender: Option<DataEventSender>,
    event_handler: Option<Arc<dyn DataEventHandler>>,

    // CAN frame cache
    frame_cache: Arc<RwLock<CanFrameCache>>,

    // Point manager
    point_manager: Arc<PointManager>,

    // Cached data (latest values)
    cached_data: Arc<RwLock<HashMap<u32, DataPoint>>>,
}

impl CanClient {
    /// Create a new CAN client with the given configuration.
    pub fn new(config: CanConfig) -> Self {
        let point_manager = PointManager::new();

        Self {
            config,
            connection_state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            is_connected: Arc::new(AtomicBool::new(false)),
            read_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            last_error: Arc::new(RwLock::new(None)),
            receive_handle: None,
            read_handle: None,
            event_sender: None,
            event_handler: None,
            frame_cache: Arc::new(RwLock::new(CanFrameCache::new())),
            point_manager: Arc::new(point_manager),
            cached_data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add CAN points to the client.
    /// This should be called after `new()` and before `connect()`.
    pub fn add_points(&mut self, points: Vec<super::config::CanPoint>) {
        #[cfg(feature = "tracing-support")]
        tracing::info!("Adding {} CAN points to client", points.len());
        
        let point_manager = Arc::get_mut(&mut self.point_manager)
            .expect("PointManager should be uniquely owned before connect()");
        
        for point in points {
            #[cfg(feature = "tracing-support")]
            tracing::debug!(
                "Adding point {}: CAN_ID=0x{:03X}, byte_offset={}, bit_pos={}, bit_len={}",
                point.point_id, point.can_id, point.byte_offset, point.bit_position, point.bit_length
            );
            
            point_manager.add_point(point);
        }
        
        #[cfg(feature = "tracing-support")]
        tracing::info!("CAN points added successfully");
    }

    /// Start the CAN frame receive task.
    fn start_receive_task(&mut self) -> Result<()> {
        let can_interface = self.config.can_interface.clone();
        let is_connected = Arc::clone(&self.is_connected);
        let frame_cache = Arc::clone(&self.frame_cache);
        let error_count = Arc::new(AtomicU64::new(0));
        let last_error = Arc::clone(&self.last_error);
        let rx_poll_interval = self.config.rx_poll_interval_ms;

        let handle = tokio::spawn(async move {
            #[cfg(feature = "tracing-support")]
            tracing::info!("Starting CAN socket open on interface: {}", can_interface);
            
            let socket = match CanSocket::open(&can_interface) {
                Ok(s) => {
                    #[cfg(feature = "tracing-support")]
                    tracing::info!("CAN socket opened successfully on {}", can_interface);
                    s
                }
                Err(e) => {
                    #[cfg(feature = "tracing-support")]
                    tracing::error!("Failed to open CAN socket on {}: {}", can_interface, e);
                    
                    *last_error.write().await =
                        Some(format!("Failed to open CAN socket: {}", e));
                    error_count.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };

            // Set non-blocking mode
            if let Err(e) = socket.set_nonblocking(true) {
                #[cfg(feature = "tracing-support")]
                tracing::error!("Failed to set non-blocking mode: {}", e);
                
                *last_error.write().await =
                    Some(format!("Failed to set non-blocking mode: {}", e));
                error_count.fetch_add(1, Ordering::Relaxed);
                return;
            }

            #[cfg(feature = "tracing-support")]
            tracing::info!("CAN socket configured successfully, starting receive loop");
            
            #[cfg(feature = "tracing-support")]
            tracing::info!("CAN receive task started on {} (rx_poll_interval={}ms)", can_interface, rx_poll_interval);

            #[cfg(feature = "tracing-support")]
            tracing::info!("Creating interval with {}ms period...", rx_poll_interval);
            
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(rx_poll_interval));

            #[cfg(feature = "tracing-support")]
            tracing::info!("Interval created, starting receive loop");

            let mut poll_count = 0u64;
            loop {
                #[cfg(feature = "tracing-support")]
                {
                    poll_count += 1;
                    if poll_count == 1 {
                        tracing::info!("First tick - waiting for interval...");
                    }
                }
                
                interval.tick().await;
                
                #[cfg(feature = "tracing-support")]
                {
                    if poll_count == 1 {
                        tracing::info!("First tick completed! Loop is working.");
                    }
                    if poll_count % 20 == 0 {
                        tracing::debug!("CAN receive loop: {} polls, checking for frames...", poll_count);
                    }
                }

                if !is_connected.load(Ordering::SeqCst) {
                    #[cfg(feature = "tracing-support")]
                    tracing::info!("CAN receive loop stopping (disconnected)");
                    break;
                }

                // Try to read a CAN frame
                match socket.read_frame() {
                    Ok(frame) => {
                        // Use socketcan Frame trait's raw_id() method
                        let can_id = frame.raw_id();

                        #[cfg(feature = "tracing-support")]
                        tracing::info!(
                            "Raw CAN frame received: ID=0x{:03X} ({}), checking if LYNK...",
                            can_id,
                            can_id
                        );

                        // Check if this is a LYNK protocol frame
                        if LynkCanId::is_lynk_id(can_id) {
                            let data = frame.data().to_vec();

                            #[cfg(feature = "tracing-support")]
                            tracing::info!(
                                "Received LYNK CAN frame: ID=0x{:03X}, Data={:02X?}",
                                can_id,
                                data
                            );

                            frame_cache.write().await.update(can_id, data);
                        } else {
                            #[cfg(feature = "tracing-support")]
                            tracing::warn!(
                                "Ignoring non-LYNK CAN frame: ID=0x{:03X}",
                                can_id
                            );
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        // No data available, continue polling
                        // This is normal in non-blocking mode
                        continue;
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing-support")]
                        tracing::error!("CAN read error: {:?}", e);
                        
                        *last_error.write().await = Some(format!("CAN read error: {}", e));
                        error_count.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }

            #[cfg(feature = "tracing-support")]
            tracing::info!("CAN receive task stopped");
        });

        self.receive_handle = Some(handle);
        Ok(())
    }

    /// Start the data reading task.
    fn start_read_task(&mut self) -> Result<()> {
        let is_connected = Arc::clone(&self.is_connected);
        let frame_cache = Arc::clone(&self.frame_cache);
        let point_manager = Arc::clone(&self.point_manager);
        let cached_data = Arc::clone(&self.cached_data);
        let read_count = Arc::new(AtomicU64::new(0));
        let error_count = Arc::new(AtomicU64::new(0));
        let last_error = Arc::clone(&self.last_error);
        let event_sender = self.event_sender.clone();
        let event_handler = self.event_handler.clone();
        let read_interval = self.config.data_read_interval_ms;

        let handle = tokio::spawn(async move {
            #[cfg(feature = "tracing-support")]
            tracing::info!("CAN data reading task started");

            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_millis(read_interval));

            loop {
                interval.tick().await;

                if !is_connected.load(Ordering::SeqCst) {
                    break;
                }

                // Apply mappings to decode cached frames
                let cache = frame_cache.read().await;
                
                #[cfg(feature = "tracing-support")]
                {
                    tracing::info!("Frame cache has {} CAN IDs", cache.len());
                    for (can_id, data) in cache.iter() {
                        tracing::debug!("  CAN ID 0x{:03X}: {} bytes", can_id, data.len());
                    }
                }
                
                match point_manager.apply_mappings(&cache) {
                    Ok(decoded_points) => {
                        #[cfg(feature = "tracing-support")]
                        tracing::info!("Decoded {} points from frame cache", decoded_points.len());
                        
                        if decoded_points.is_empty() {
                            #[cfg(feature = "tracing-support")]
                            tracing::warn!("No points decoded from frame cache");
                            continue;
                        }

                        let timestamp = chrono::Utc::now();
                        let mut batch = DataBatch::new();

                        for (point_id, (value, data_type, quality)) in decoded_points {
                            #[cfg(feature = "tracing-support")]
                            tracing::debug!("  Point {}: {:?} (type: {:?}, quality: {:?})", point_id, value, data_type, quality);
                            
                            let data_point = DataPoint {
                                id: point_id,
                                data_type,
                                value,
                                quality,
                                timestamp,
                                source_timestamp: None,
                            };

                            batch.add(data_point.clone());

                            // Update cache
                            cached_data.write().await.insert(point_id, data_point);
                        }

                        if !batch.is_empty() {
                            read_count.fetch_add(1, Ordering::Relaxed);

                            #[cfg(feature = "tracing-support")]
                            tracing::info!("Sending batch with {} data points to event system", batch.len());

                            // Send event
                            if let Some(ref sender) = event_sender {
                                #[cfg(feature = "tracing-support")]
                                tracing::debug!("Sending DataUpdate event via event_sender");
                                let _ = sender.send(DataEvent::DataUpdate(batch.clone())).await;
                            } else {
                                #[cfg(feature = "tracing-support")]
                                tracing::warn!("No event_sender available");
                            }

                            // Call handler
                            if let Some(ref handler) = event_handler {
                                #[cfg(feature = "tracing-support")]
                                tracing::debug!("Calling on_data_update handler");
                                handler.on_data_update(batch).await;
                            } else {
                                #[cfg(feature = "tracing-support")]
                                tracing::warn!("No event_handler available");
                            }
                        }
                    }
                    Err(e) => {
                        #[cfg(feature = "tracing-support")]
                        tracing::error!("Failed to apply mappings: {}", e);
                        *last_error.write().await =
                            Some(format!("Failed to apply mappings: {}", e));
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            #[cfg(feature = "tracing-support")]
            tracing::info!("CAN data reading task stopped");
        });

        self.read_handle = Some(handle);
        Ok(())
    }
}

// ============================================================================
// Trait Implementations
// ============================================================================

impl ProtocolCapabilities for CanClient {
    fn name(&self) -> &'static str {
        "CAN"
    }

    fn supported_modes(&self) -> &[CommunicationMode] {
        &[CommunicationMode::EventDriven]
    }

    fn supports_client(&self) -> bool {
        true
    }

    fn supports_server(&self) -> bool {
        false
    }

    fn version(&self) -> &'static str {
        "LYNK Protocol"
    }
}

impl Protocol for CanClient {
    fn connection_state(&self) -> ConnectionState {
        *futures::executor::block_on(self.connection_state.read())
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResponse> {
        let cached = self.cached_data.read().await;

        let mut batch = DataBatch::new();

        match (&request.data_type, &request.point_ids) {
            (None, None) => {
                // Return all cached data
                for point in cached.values() {
                    batch.add(point.clone());
                }
            }
            (Some(dtype), None) => {
                for point in cached.values() {
                    if point.data_type == *dtype {
                        batch.add(point.clone());
                    }
                }
            }
            (None, Some(ids)) => {
                for id in ids {
                    if let Some(point) = cached.get(id) {
                        batch.add(point.clone());
                    }
                }
            }
            (Some(dtype), Some(ids)) => {
                for id in ids {
                    if let Some(point) = cached.get(id) {
                        if point.data_type == *dtype {
                            batch.add(point.clone());
                        }
                    }
                }
            }
        }

        Ok(ReadResponse::success(batch))
    }

    async fn diagnostics(&self) -> Result<Diagnostics> {
        Ok(Diagnostics {
            protocol: "CAN".to_string(),
            connection_state: *self.connection_state.read().await,
            read_count: self.read_count.load(Ordering::Relaxed),
            write_count: 0,
            error_count: self.error_count.load(Ordering::Relaxed),
            last_error: self.last_error.read().await.clone(),
            extra: serde_json::json!({
                "can_interface": self.config.can_interface,
                "bitrate": self.config.bitrate,
            }),
        })
    }
}

impl ProtocolClient for CanClient {
    async fn connect(&mut self) -> Result<()> {
        *self.connection_state.write().await = ConnectionState::Connecting;

        // Verify CAN interface exists
        let _socket = CanSocket::open(&self.config.can_interface).map_err(|e| {
            GatewayError::Connection(format!(
                "Failed to open CAN interface {}: {}",
                self.config.can_interface, e
            ))
        })?;

        #[cfg(feature = "tracing-support")]
        tracing::info!("CAN interface {} opened successfully", self.config.can_interface);

        self.is_connected.store(true, Ordering::SeqCst);
        *self.connection_state.write().await = ConnectionState::Connected;

        // Start receive and read tasks
        self.start_receive_task()?;
        self.start_read_task()?;

        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.is_connected.store(false, Ordering::SeqCst);

        // Stop receive task
        if let Some(handle) = self.receive_handle.take() {
            handle.abort();
        }

        // Stop read task
        if let Some(handle) = self.read_handle.take() {
            handle.abort();
        }

        *self.connection_state.write().await = ConnectionState::Disconnected;

        #[cfg(feature = "tracing-support")]
        tracing::info!("CAN client disconnected");

        Ok(())
    }

    async fn poll_once(&mut self) -> Result<DataBatch> {
        // CAN protocol is event-driven, return current cached data
        let cached = self.cached_data.read().await;
        let mut batch = DataBatch::new();
        for point in cached.values() {
            batch.add(point.clone());
        }
        Ok(batch)
    }

    async fn write_control(&mut self, _commands: &[ControlCommand]) -> Result<WriteResult> {
        Err(GatewayError::Unsupported(
            "Write control not supported for CAN protocol".to_string(),
        ))
    }

    async fn write_adjustment(&mut self, _adjustments: &[AdjustmentCommand]) -> Result<WriteResult> {
        Err(GatewayError::Unsupported(
            "Write adjustment not supported for CAN protocol".to_string(),
        ))
    }

    async fn start_polling(&mut self, _config: PollingConfig) -> Result<()> {
        // CAN is event-driven, polling is not applicable
        Err(GatewayError::Unsupported(
            "Polling mode not supported for CAN protocol (use event-driven mode)".to_string(),
        ))
    }

    async fn stop_polling(&mut self) -> Result<()> {
        // CAN is event-driven, polling is not applicable
        Ok(())
    }
}

impl EventDrivenProtocol for CanClient {
    fn subscribe(&self) -> DataEventReceiver {
        let (_tx, rx) = mpsc::channel(100);
        // Note: This is a workaround for &self constraint - in production, use interior mutability
        // For now, just return a receiver
        rx
    }

    fn set_event_handler(&mut self, handler: Arc<dyn DataEventHandler>) {
        self.event_handler = Some(handler);
    }
}

