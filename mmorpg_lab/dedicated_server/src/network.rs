use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared::broker_protocol::{BrokerMessage, string_to_topic};
use shared::{ClientMessage, PlayerState, ServerMessage};
use std::collections::HashMap;
use tracing::{error, info, warn};

use crate::ServerConfig;

pub struct PlayerData {
    pub username: String,
    pub position: Vec2,
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<u32, PlayerData>, // Maps client IDs (u32 given by the broker, matching the game_socket connection Uuid) to player data
}

#[derive(Resource)]
pub struct NetworkManager {
    pub peer: GamePeer,
    pub broker_connection: Option<GameConnection>,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<PlayerRegistry>().add_systems(
            Update,
            (poll_network_events, broadcast_aoi_and_positions).chain(),
        );
    }
}

fn poll_network_events(mut net: ResMut<NetworkManager>, mut registry: ResMut<PlayerRegistry>) {
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            // Connection event WITH THE BROKER
            GameNetworkEvent::Connected(connection) => {
                info!(
                    "[NETWORK] Connected to Broker: {:?}",
                    connection.connection_id
                );
                net.broker_connection = Some(connection);

                // Ask gamesocket to open our 2 communication lanes with broker
                if let Err(e) = net
                    .peer
                    .create_stream(connection, GameStreamReliability::Reliable)
                {
                    error!("Failed to create reliable stream: {:?}", e);
                }
                if let Err(e) = net
                    .peer
                    .create_stream(connection, GameStreamReliability::Unreliable)
                {
                    error!("Failed to create unreliable stream: {:?}", e);
                }
            }
            // Broker lanes are ready
            GameNetworkEvent::StreamCreated(_connection, stream) => {
                if stream.is_reliable() {
                    info!("[NETWORK] Reliable stream to Broker is ready.");
                    net.reliable_stream = Some(stream);
                } else {
                    info!("[NETWORK] Unreliable stream to Broker is ready.");
                    net.unreliable_stream = Some(stream);
                }
            }
            // Broker lanes are closed
            GameNetworkEvent::StreamClosed(_connection, stream) => {
                if stream.is_reliable() {
                    info!("[NETWORK] Reliable stream to Broker is closed.");
                    net.reliable_stream = None;
                } else {
                    info!("[NETWORK] Unreliable stream to Broker is closed.");
                    net.unreliable_stream = None;
                }
            }
            GameNetworkEvent::Disconnected(_connection) => {
                error!("[NETWORK] Lost connection to the Broker!");
                net.broker_connection = None;
            }
            // Receiving messages FROM THE BROKER
            GameNetworkEvent::Message { data, .. } => {
                // First, unwrap the Broker protocol envelope
                if let Some(broker_msg) = BrokerMessage::from_bytes(&data) {
                    handle_broker_message(broker_msg, &mut registry);
                } else {
                    warn!("[NETWORK] Received malformed Broker message.");
                }
            }
            _ => {}
        }
    }
}

// ----------------------------------------------------------------------------------------------------------------------------------------
// Read and handle messages from the Broker. For now : JOIN and MOVE_INPUT from clients, wrapped in the BrokerMessage::ClientInput variant.
// ----------------------------------------------------------------------------------------------------------------------------------------
fn handle_broker_message(message: BrokerMessage, registry: &mut PlayerRegistry) {
    match message {
        // The Broker routes inputs to the game server
        // It contains the client_id (u32) and the payload (16 bytes).
        BrokerMessage::ClientInput { client_id, input } => {
            // Decode the inner game payload
            if let Ok(client_msg) = bincode::deserialize::<ClientMessage>(&input) {
                match client_msg {
                    ClientMessage::Join { username } => {
                        let clean_username = String::from_utf8_lossy(&username)
                            .trim_end_matches('\0')
                            .to_string();

                        info!(
                            "[GAME] Player {} (ID: {}) joined the shard!",
                            clean_username, client_id
                        );

                        registry.players.insert(
                            client_id,
                            PlayerData {
                                username: clean_username,
                                position: Vec2::ZERO,
                            },
                        );
                        // TODO: Welcome message logic is skipped here.
                        // Usually, the Spatial Server handles telling the client where they are.
                    }
                    ClientMessage::MoveInput { x, y } => {
                        if let Some(player) = registry.players.get_mut(&client_id) {
                            let speed = 5.0;
                            player.position.x += x * speed;
                            player.position.y += y * speed;
                        } else {
                            // If player doesn't exist, we might have missed the Join message
                            // Implicitly spawn them for safety
                            registry.players.insert(
                                client_id,
                                PlayerData {
                                    username: "Ghost".to_string(),
                                    position: Vec2::ZERO,
                                },
                            );
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

// -------------------------------------------------------------------------
// AOI & Spatial Updates
// -------------------------------------------------------------------------
fn broadcast_aoi_and_positions(
    net: ResMut<NetworkManager>,
    registry: Res<PlayerRegistry>,
    config: Res<ServerConfig>,
) {
    let Some(broker_conn) = &net.broker_connection else {
        return;
    };
    let Some(unrel_stream) = &net.unreliable_stream else {
        return;
    };

    let mut all_players = Vec::new();

    // ============ Send Position Updates for the Spatial Server (Tag 0x10) ===========
    for (client_id, player_data) in &registry.players {
        all_players.push(PlayerState {
            id: *client_id,
            username: player_data.username.clone(),
            x: player_data.position.x,
            y: player_data.position.y,
        });

        // Notify the Spatial Server of the exact coordinates
        let pos_update = BrokerMessage::PositionUpdate {
            client_id: *client_id,
            x: player_data.position.x,
            y: player_data.position.y,
        };

        let _ = net.peer.send(
            broker_conn,
            unrel_stream,
            Bytes::from(pos_update.to_bytes()),
        );
    }

    if all_players.is_empty() {
        return;
    }

    // ============ Publish the Global AOI of this Shard (Tag 0x03) ===========
    let snapshot = ServerMessage::AOISnapshot {
        players: all_players,
    };

    match bincode::serialize(&snapshot) {
        Ok(payload) => {
            let publish_msg = BrokerMessage::Publish {
                topic: string_to_topic(&config.zone),
                payload,
            };

            if let Err(e) = net.peer.send(
                broker_conn,
                unrel_stream,
                Bytes::from(publish_msg.to_bytes()),
            ) {
                warn!("Failed to publish AOI to Broker: {:?}", e);
            }
        }
        Err(e) => error!("Failed to serialize AOI Snapshot: {:?}", e),
    }
}
