use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream};
use shared::{ClientMessage, PlayerState, ServerMessage};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

pub struct PlayerData {
    pub username: String,
    pub position: Vec2,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
}

#[derive(Resource, Default)]
pub struct PlayerRegistry {
    pub players: HashMap<Uuid, PlayerData>, // Maps client IDs (Uuid given by game_sockets) to player data
}

#[derive(Resource)]
pub struct NetworkManager {
    pub peer: GamePeer,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<PlayerRegistry>()
            .add_systems(Update, (poll_network_events, broadcast_aoi).chain());
    }
}

fn poll_network_events(mut net: ResMut<NetworkManager>, mut registry: ResMut<PlayerRegistry>) {
    while let Ok(Some(event)) = net.peer.poll() {
        match event {
            GameNetworkEvent::Connected(connection) => {
                info!("[NETWORK] Client connected: {:?}", connection.connection_id);
                registry.players.insert(
                    connection.connection_id,
                    PlayerData {
                        username: "Unknown".to_string(), // pseudo unknown at connection time, will be updated when receiving the join message from the client
                        position: Vec2::ZERO,
                        reliable_stream: None,
                        unreliable_stream: None,
                    },
                );
            }
            GameNetworkEvent::StreamCreated(connection, stream) => {
                if let Some(player) = registry.players.get_mut(&connection.connection_id) {
                    if stream.is_reliable() {
                        player.reliable_stream = Some(stream);
                    } else {
                        player.unreliable_stream = Some(stream);
                    }
                }
            }
            GameNetworkEvent::StreamClosed(connection, stream) => {
                if let Some(player) = registry.players.get_mut(&connection.connection_id) {
                    if stream.is_reliable() {
                        player.reliable_stream = None;
                    } else {
                        player.unreliable_stream = None;
                    }
                }
            }
            GameNetworkEvent::Disconnected(connection) => {
                info!(
                    "[NETWORK] Client disconnected: {}",
                    connection.connection_id
                );
                registry.players.remove(&connection.connection_id);
            }
            GameNetworkEvent::Message {
                connection,
                stream: _,
                data,
            } => {
                handle_client_message(&connection, &data, &mut registry, &mut net);
            }
            _ => {}
        }
    }
}

// -------------------------------------------------------------------------
// Read and handle messages from clients. For now : JOIN and MOVE_INPUT
// -------------------------------------------------------------------------
fn handle_client_message(
    connection: &GameConnection,
    data: &[u8],
    registry: &mut PlayerRegistry,
    net: &mut NetworkManager,
) {
    match bincode::deserialize::<ClientMessage>(data) {
        Ok(message) => match message {
            ClientMessage::Join { username } => {
                info!("[GAME] Player {} joined the game !", username);
                if let Some(player) = registry.players.get_mut(&connection.connection_id) {
                    player.username = username;

                    // Send welcome message with player ID
                    let welcome_msg = ServerMessage::Welcome {
                        player_id: connection.connection_id,
                    };
                    match bincode::serialize(&welcome_msg) {
                        Ok(bytes) => {
                            if let Some(rel_stream) = &player.reliable_stream {
                                if let Err(e) =
                                    net.peer.send(connection, rel_stream, Bytes::from(bytes))
                                {
                                    error!("Failed to send Welcome message: {:?}", e);
                                }
                            }
                        }
                        Err(e) => error!("Failed to serialize Welcome message: {:?}", e),
                    }
                }
            }
            ClientMessage::MoveInput { x, y } => {
                debug!(
                    "Input received from {:?} : x={}, y={}",
                    connection.connection_id, x, y
                );
                if let Some(player) = registry.players.get_mut(&connection.connection_id) {
                    let speed = 5.0;
                    player.position.x += x * speed;
                    player.position.y += y * speed;
                }
            }
        },
        Err(e) => warn!(
            "[SECURITY] Wrong packet received from {:?} : {}",
            connection.connection_id, e
        ),
    }
}

// Area of interest (AOI) system : every tick, send each player a custom snapshot of all players that are within 400 pixels of them
fn broadcast_aoi(net: ResMut<NetworkManager>, registry: Res<PlayerRegistry>) {
    let camera_view_distance = 400.0; // AOI radius in pixels

    for (client_id, player_data) in &registry.players {
        let Some(unreliable_stream) = &player_data.unreliable_stream else {
            continue;
        };

        let mut visible_players = Vec::new();
        for (other_id, other_data) in &registry.players {
            if player_data.position.distance(other_data.position) < camera_view_distance {
                visible_players.push(PlayerState {
                    id: *other_id,
                    username: other_data.username.clone(),
                    x: other_data.position.x,
                    y: other_data.position.y,
                });
            }
        }

        let snapshot = ServerMessage::AOISnapshot {
            players: visible_players,
        };
        match bincode::serialize(&snapshot) {
            Ok(bytes) => {
                let conn = game_sockets::GameConnection {
                    connection_id: *client_id,
                };
                if let Err(e) = net.peer.send(&conn, unreliable_stream, Bytes::from(bytes)) {
                    // unreliable datagram : if it fails to send, we just skip it and wait for the next one, no big deal
                    warn!("Impossible d'envoyer l'AOI à {:?}: {:?}", client_id, e);
                }
            }
            Err(e) => error!("Erreur de sérialisation du Snapshot: {:?}", e),
        }
    }
}
