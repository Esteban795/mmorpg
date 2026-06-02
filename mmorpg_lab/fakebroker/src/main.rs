use bytes::Bytes;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;
use shared::broker_protocol::BrokerMessage;
fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    let backend = QuicBackend::new();
    let mut peer = GamePeer::new(backend);
    if let Err(e) = peer.listen("127.0.0.1", 10000) {
        error!("CRITICAL: Failed to listen on port {}: {:?}", 10000, e);
        return;
    }

    let mut reliable_stream: Option<GameStream> = None;
    let mut unreliable_stream: Option<GameStream> = None;
    let mut conn: Option<GameConnection> = None;
    loop {
        while let Ok(Some(event)) = peer.poll() {
            match event {
                GameNetworkEvent::Connected(connection) => {
                    info!("[NETWORK] Client connected: {:?}", connection.connection_id);
                    conn = Some(connection);
                    peer.create_stream(connection, GameStreamReliability::Reliable);
                    peer.create_stream(connection, GameStreamReliability::Unreliable);
                }
                GameNetworkEvent::StreamCreated(connection, stream) => {
                    info!(
                        "[NETWORK] Stream created for client {}: reliable={}",
                        connection.connection_id,
                        stream.is_reliable()
                    );
                    match stream.is_reliable() {
                        true => {
                            reliable_stream = Some(stream);
                            if let Some(rel_stream) = reliable_stream {
                                let msg = BrokerMessage::PositionUpdate { client_id: 12, x: 12.0, y: 12.0 };
                                if let Err(e) = peer.send(
                                    &connection,
                                    &rel_stream,
                                    Bytes::from(msg.to_bytes()))
                                {
                                    error!("Failed to send message on reliable stream: {:?}", e);
                                }
                            }
                        }
                        false => {
                            unreliable_stream = Some(stream);
                            if let Some(unrel_stream) = unreliable_stream {

                                let msg = BrokerMessage::PositionUpdate { client_id: 12, x: 12.0, y: 12.0 };
                                if let Err(e) = peer.send(
                                    &connection,
                                    &unrel_stream,
                                    Bytes::from(msg.to_bytes()))
                                {
                                    error!("Failed to send message on unreliable stream: {:?}", e);
                                }
                            }
                        }
                    }
                }
                GameNetworkEvent::StreamClosed(connection, stream) => {
                    info!(
                        "[NETWORK] Stream closed for client {}: reliable={}",
                        connection.connection_id,
                        stream.is_reliable()
                    );
                }
                GameNetworkEvent::Disconnected(connection) => {
                    info!(
                        "[NETWORK] Client disconnected: {}",
                        connection.connection_id
                    );
                }
                GameNetworkEvent::Message {
                    connection,
                    stream: _,
                    data,
                } => {
                    info!(
                        "[NETWORK] Message received from client {}: {} bytes",
                        connection.connection_id,
                        data.len()
                    );
                    // handle_client_message(&connection, &data, &mut registry, &mut net);
                }
                _ => {}
            }
        }
    }
}
