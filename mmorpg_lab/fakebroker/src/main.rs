use std::collections::HashMap;

use bytes::Bytes;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use shared::broker_protocol::{BrokerMessage, topic_to_string};
use tracing::{Level, error, info, warn};
use tracing_subscriber::FmtSubscriber;

struct Player {
    id: u32,
    x: f32,
    y: f32,
    factor_x: f32,
    factor_y: f32,
}

struct PlayerRegistry {
    players: HashMap<u32, Player>,
}

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    let backend = QuicBackend::new();
    let mut peer = GamePeer::new(backend);
    if let Err(e) = peer.listen("127.0.0.1", 10001) {
        error!("CRITICAL: Failed to listen on port {}: {:?}", 10001, e);
        return;
    }

    let mut reliable_stream: Option<GameStream> = None;
    let mut unreliable_stream: Option<GameStream> = None;
    let mut conn: Option<GameConnection> = None;

    let mut should_send = false;
    let mut count = 0;

    let mut player_registry = PlayerRegistry {
        players: HashMap::new(),
    };

    player_registry.players.insert(
        0,
        Player {
            id: 0,
            x: -250.0,
            y: -250.0,
            factor_x: 10.0,
            factor_y: 0.0,
        },
    );

    player_registry.players.insert(
        1,
        Player {
            id: 1,
            x: 250.0,
            y: -250.0,
            factor_x: 0.0,
            factor_y: 0.0,
        },
    );

    player_registry.players.insert(
        2,
        Player {
            id: 2,
            x: -250.0,
            y: 250.0,
            factor_x: 0.0,
            factor_y: 0.0,
        },
    );

    player_registry.players.insert(
        3,
        Player {
            id: 3,
            x: 250.0,
            y: 250.0,
            factor_x: 0.0,
            factor_y: 0.0,
        },
    );

    let player_count = player_registry.players.len();

    let mut temp = 0;
    let mut shard_index = 1;
    let mut maxcount = 5;
    loop {
        if should_send {
            // let client_id = (count % player_count) as u32;
            let mut client_id = 0;
            if maxcount > 0 {
                client_id = (count % player_count) as u32;
                maxcount -= 1;
            }
            let player = player_registry.players.get_mut(&client_id).unwrap();

            if maxcount <= 0 {
                if player.x > 40.0 {
                    player.factor_x = -10.0;
                }
            }
            let update_msg = BrokerMessage::PositionUpdate {
                client_id,
                x: player.x,
                y: player.y,
                score: 0f32,
            };

            if let Some(connection) = &conn {
                info!(
                    "Sending PositionUpdate to client {}: x={}, y={} on connection {:?}",
                    client_id, player.x, player.y, connection
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

            if temp % 1000 == 0 && maxcount <= 0 {
                let shard_ready_msg = BrokerMessage::ShardReady {
                    shard_id: shard_index,
                };
                shard_index += 1;
                if let Some(connection) = &conn {
                    info!(
                        "Sending ShardReady {{ shard_id: {} }} to client {} on connection {:?}",
                        shard_index, client_id, connection
                    );
                    if let Some(ref unrel_stream) = unreliable_stream {
                        if let Err(e) = peer.send(
                            &connection,
                            &unrel_stream,
                            Bytes::from(shard_ready_msg.to_bytes()),
                        ) {
                            error!("Failed to send message on unreliable stream: {:?}", e);
                        }
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

                    if let Ok(_) = peer.create_stream(connection, GameStreamReliability::Unreliable)
                    {
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
                            // if let Some(ref rel_stream) = reliable_stream {
                            //     let msg = BrokerMessage::PositionUpdate {
                            //         client_id: 12,
                            //         x: x,
                            //         y: y,
                            //     };
                            //     if let Err(e) =
                            //         peer.send(&connection, &rel_stream, Bytes::from(msg.to_bytes()))
                            //     {
                            //         error!("Failed to send message on reliable stream: {:?}", e);
                            //     }
                            // }
                        }
                        false => {
                            unreliable_stream = Some(stream);
                            // if let Some(ref unrel_stream) = unreliable_stream {
                            //     let msg = BrokerMessage::PositionUpdate {
                            //         client_id: 12,
                            //         x: x,
                            //         y: y,
                            //     };
                            //     if let Err(e) = peer.send(
                            //         &connection,
                            //         &unrel_stream,
                            //         Bytes::from(msg.to_bytes()),
                            //     ) {
                            //         error!("Failed to send message on reliable stream: {:?}", e);
                            //     }
                            // }
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
                    stream: _,
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
                            }
                            BrokerMessage::Unsubscribe { client_id, topic } => {
                                let topic_str = topic_to_string(&topic);
                                info!(
                                    "Received Unsubscribe from client {}: topic={}",
                                    client_id, topic_str
                                );
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
        std::thread::sleep(std::time::Duration::from_millis(100));
        let index = (count % player_count) as u32;
        let player = player_registry.players.get_mut(&index).unwrap();
        player.x += player.factor_x;
        player.y += player.factor_y;
        count += 1;
    }
}
