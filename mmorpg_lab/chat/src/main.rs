pub mod moderator;
pub mod chat_service;

use bytes::Bytes;
use game_sockets::{GameNetworkEvent, GamePeer, GameStreamReliability, protocols::QuicBackend};
use shared::{DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT, broker_protocol::BrokerMessage};
use tracing::{Level, error, info, warn};
use tracing_subscriber::FmtSubscriber;

use crate::moderator::Moderator;

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    info!("Starting chat server...");

    let backend = QuicBackend::new();
    let peer = GamePeer::new(backend);

    let broker_addr: String = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| DEFAULT_BROKER_IP.to_string())
        .parse()
        .expect("Invalid BROKER_ADDR");

    let broker_port: u16 = std::env::var("BROKER_PORT")
        .unwrap_or_else(|_| DEFAULT_BROKER_PORT.to_string())
        .parse()
        .expect("Invalid BROKER_PORT");

    let moderator = Moderator::new("badwords.txt");

    if let Err(e) = peer.connect(&broker_addr, broker_port) {
        error!("Failed to connect to broker: {:?}", e);
        return;
    }

    run(moderator, peer);
}

fn run(moderator: Moderator, mut peer: GamePeer) {
    loop {
        while let Ok(Some(event)) = peer.poll() {
            match event {
                GameNetworkEvent::Connected(connection) => {
                    info!(" Connected to broker : {:?}", connection.connection_id);
                    if let Err(e) = peer.create_stream(connection, GameStreamReliability::Reliable)
                    {
                        error!("Failed to create reliable stream for broker: {:?}", e);
                        return;
                    }
                }
                GameNetworkEvent::StreamCreated(connection, stream) => {
                    info!(
                        " Stream created for broker {:?}, reliable: {}",
                        connection.connection_id,
                        stream.is_reliable()
                    );
                }
                GameNetworkEvent::StreamClosed(connection, stream) => {
                    error!(
                        " Stream closed for broker {:?}, reliable: {}",
                        connection.connection_id,
                        stream.is_reliable()
                    );
                }
                GameNetworkEvent::Disconnected(game_connection) => {
                    error!(
                        "Disconnected from broker: {:?}",
                        game_connection.connection_id
                    );
                }
                GameNetworkEvent::Message { data, .. } => {
                    let msg_opt = BrokerMessage::from_bytes(data.as_ref());

                    if let Some(msg) = msg_opt {
                        info!("Received message from broker: {:?}", msg);

                        match msg {
                            BrokerMessage::ClientChatMessage { client_id, msg } => {
                                let str_msg = String::from_utf8_lossy(&msg);
                                info!(
                                    "Received chat message from client {}: {:?}",
                                    client_id, str_msg
                                );

                                let moderated_msg = moderator.moderate_message(&str_msg);

                                let broadcast_msg = BrokerMessage::BroadcastChatMessage {
                                    username: format!("Player{}", client_id).into_bytes(),
                                    msg: moderated_msg.into_bytes(),
                                };
                            }
                            _ => {
                                warn!(
                                    "Received unsupported message type in chat server: {:?}",
                                    msg
                                );
                            }
                        }
                    } else {
                        warn!("Received invalid message from broker: {:?}", data);
                    }
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
