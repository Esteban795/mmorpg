use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use shared::orchestrator_protocol::OrchestratorMessage;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

pub struct QuicOrchestrator {
    pub peer: GamePeer,
    pub connection: Option<GameConnection>,
    pub reliable_stream: Option<GameStream>,
    spawn_tx: mpsc::UnboundedSender<u32>,
}

impl QuicOrchestrator {
    pub fn new(addr: &String, port: u16, spawn_tx: mpsc::UnboundedSender<u32>) -> Self {
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
                        self.handle_message(&connection, &data);
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

    fn handle_message(&mut self, conn: &GameConnection, data: &[u8]) {
        if let Some(message) = OrchestratorMessage::from_bytes(data) {
            match message {
                OrchestratorMessage::RequestSplit {
                    shard_id,
                    new_shards_ids,
                } => {
                    for new_id in new_shards_ids {
                        if let Err(e) = self.spawn_tx.send(new_id) {
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
