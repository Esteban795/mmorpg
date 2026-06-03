use bytes::Bytes;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use shared::broker_protocol::{BrokerMessage, topic_to_string};
use tracing::{Level, error, info, warn};
use tracing_subscriber::FmtSubscriber;
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

    let mut x = 12.0;
    let mut y = 12.0;
    let mut should_send = false;
    loop {
        if should_send && x % 1000.0 == 0.0 {
            let client_id = 12;
            let update_msg = BrokerMessage::PositionUpdate { client_id, x, y };

            if let Some(connection) = &conn {
                info!(
                    "Sending PositionUpdate to client {}: x={}, y={} on connection {:?}",
                    client_id, x, y, connection
                );
                if let Some(ref unrel_stream) = unreliable_stream {
                    if let Err(e) = peer.send(
                        &connection,
                        &unrel_stream,
                        Bytes::from(update_msg.to_bytes()),
                    ) {
                        error!("Failed to send message on unreliable stream: {:?}", e);
                    }
                }
            }
        }
        while let Ok(Some(event)) = peer.poll() {
            match event {
                GameNetworkEvent::Connected(connection) => {
                    info!("[NETWORK] Client connected: {:?}", connection.connection_id);

                    conn = Some(connection);
                    if let Ok(_) = peer.create_stream(connection, GameStreamReliability::Reliable) {
                        info!(
                            "Reliable stream created for client {}",
                            connection.connection_id
                        );
                    } else {
                        error!(
                            "Failed to create reliable stream for client {}",
                            connection.connection_id
                        );
                    }

                    if let Ok(_) = peer.create_stream(connection, GameStreamReliability::Unreliable) {
                        info!(
                            "Unreliable stream created for client {}",
                            connection.connection_id
                        );
                    } else {
                        error!(
                            "Failed to create unreliable stream for client {}",
                            connection.connection_id
                        );
                    }
                }
                GameNetworkEvent::StreamCreated(connection, stream) => {
                    info!(
                        "[NETWORK] Stream created for client {}: reliable={},stream_id={}",
                        connection.connection_id,
                        stream.is_reliable(),
                        stream.stream_id
                    );
                    match stream.is_reliable() {
                        true => {
                            reliable_stream = Some(stream); 
                            if let Some(ref rel_stream) = reliable_stream {
                                let msg = BrokerMessage::PositionUpdate {
                                    client_id: 12,
                                    x: x,
                                    y: y,
                                };
                                if let Err(e) =
                                    peer.send(&connection, &rel_stream, Bytes::from(msg.to_bytes()))
                                {
                                    error!("Failed to send message on reliable stream: {:?}", e);
                                }
                            }  
                        }
                        false => {
                            unreliable_stream = Some(stream);
                            if let Some(ref unrel_stream) = unreliable_stream {
                                let msg = BrokerMessage::PositionUpdate {
                                    client_id: 12,
                                    x: x,
                                    y: y,
                                };
                                if let Err(e) =
                                    peer.send(&connection, &unrel_stream, Bytes::from(msg.to_bytes()))
                                {
                                    error!("Failed to send message on reliable stream: {:?}", e);
                                }
                            }
                        }
                    }
                    should_send = true;
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
                    connection: _,
                    stream : _,
                    data,
                } => {
                    let connection = conn.unwrap();
                    info!(
                        "[NETWORK] Message received from client {}: {} bytes",
                        connection.connection_id,
                        data.len()
                    );
                    if let Some(message) = BrokerMessage::from_bytes(&data) {
                        match message {
                            BrokerMessage::Subscribe { client_id, topic } => {
                                let topic_str = topic_to_string(&topic);
                                info!(
                                    "Received Subscribe from client {}: topic={}",
                                    client_id, topic_str
                                );

                                x += 10.0;
                                y += 10.0;

                                let update_msg = BrokerMessage::PositionUpdate { client_id, x, y };

                                info!(
                                    "Sending PositionUpdate to client {}: x={}, y={} on connection {:?}",
                                    client_id, x, y, connection
                                );
                                if let Some(ref unrel_stream) = unreliable_stream {
                                    if let Err(e) = peer.send(
                                        &connection,
                                        &unrel_stream,
                                        Bytes::from(update_msg.to_bytes()),
                                    ) {
                                        error!(
                                            "Failed to send message on unreliable stream: {:?}",
                                            e
                                        );
                                    }
                                }
                            }
                            _ => {
                                warn!(
                                    "Received unsupported message type from client: {:?}",
                                    message
                                );
                            }
                        }
                    } else {
                        warn!(
                            "{} {}",
                            connection.connection_id.to_string(),
                            "Received invalid message format from client"
                        );
                    }
                }
                _ => {}
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(16));
        x += 5.0;
        y += 5.0;
    }
}
