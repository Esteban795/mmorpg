use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use shared::orchestrator_protocol::OrchestratorMessage;
use shared::rect::Rect;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct QuicOrchestrator {
    pub peer: GamePeer,
    pub connection: Option<GameConnection>,
    pub reliable_stream: Option<GameStream>,
    spawn_tx: mpsc::UnboundedSender<(u32, u32, Rect)>,
}

impl QuicOrchestrator {
    pub fn new(
        addr: &String,
        port: u16,
        spawn_tx: mpsc::UnboundedSender<(u32, u32, Rect)>,
    ) -> Self {
        let backend = QuicBackend::new();
        let peer = GamePeer::new(backend);

        if let Err(e) = peer.listen(addr, port) {
            error!("CRITICAL: Failed to listen on port {}: {:?}", port, e);
        }

        Self {
            peer,
            connection: None,
            reliable_stream: None,
            spawn_tx,
        }
    }

    pub fn run(&mut self) {
        loop {
            while let Ok(Some(event)) = self.peer.poll() {
                match event {
                    GameNetworkEvent::Connected(connection) => {
                        info!("New connection established: {:?}", connection);
                        self.connection = Some(connection);

                        if let Ok(_) = self
                            .peer
                            .create_stream(connection, GameStreamReliability::Reliable)
                        {
                            info!(
                                "Reliable stream created for connection: {:?}",
                                self.connection
                            );
                        } else {
                            error!(
                                "Failed to create reliable stream for connection: {:?}",
                                self.connection
                            );
                        }
                    }
                    GameNetworkEvent::StreamCreated(connection, stream) => {
                        info!(
                            "New stream created: {:?} for connection: {:?}",
                            stream, connection
                        );
                        self.reliable_stream = Some(stream);
                    }
                    GameNetworkEvent::StreamClosed(connection, stream) => {
                        info!(
                            "Stream closed: {:?} for connection: {:?}",
                            stream, connection
                        );
                        self.reliable_stream = None;
                    }
                    GameNetworkEvent::Disconnected(connection) => {
                        info!("Connection disconnected: {:?}", connection);
                        self.connection = None;
                        self.reliable_stream = None;
                    }
                    GameNetworkEvent::Message {
                        connection,
                        stream,
                        data,
                    } => {
                        info!(
                            "Received message on stream {:?} for connection {:?}: {:?}",
                            stream, connection, data
                        );
                        self.handle_message(&data);
                    }
                    GameNetworkEvent::Error { connection, inner } => {
                        warn!(
                            " Error on connection {:?}: {:?}",
                            connection.connection_id, inner
                        );
                    }
                }
            }
        }
    }

    fn handle_message(&mut self, data: &[u8]) {
        if let Some(message) = OrchestratorMessage::from_bytes(data) {
            match message {
                OrchestratorMessage::RequestSplit {
                    shard_id,
                    new_shards_ids,
                    parent_bounds,
                } => {
                    let sub_w = parent_bounds.width / 2.0;
                    let sub_h = parent_bounds.height / 2.0;

                    let bounds = [
                        Rect {
                            x: parent_bounds.x,
                            y: parent_bounds.y,
                            width: sub_w,
                            height: sub_h,
                        }, // NW
                        Rect {
                            x: parent_bounds.x + sub_w,
                            y: parent_bounds.y,
                            width: sub_w,
                            height: sub_h,
                        }, // NE
                        Rect {
                            x: parent_bounds.x,
                            y: parent_bounds.y + sub_h,
                            width: sub_w,
                            height: sub_h,
                        }, // SW
                        Rect {
                            x: parent_bounds.x + sub_w,
                            y: parent_bounds.y + sub_h,
                            width: sub_w,
                            height: sub_h,
                        }, // SE
                    ];

                    for i in 0..4 {
                        if let Err(e) = self.spawn_tx.send((new_shards_ids[i], shard_id, bounds[i]))
                        {
                            error!("Could not send spawn request: {:?}", e);
                        }
                    }
                }
                _ => {
                    warn!("Received unsupported message type: {:?}", message);
                }
            }
        }
    }
}
