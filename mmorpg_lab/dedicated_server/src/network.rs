use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared::broker_protocol::{BrokerMessage, InterShardPayload, string_to_topic};
use shared::{MAP_BOUND_MAX, MAP_BOUND_MIN, SPAWN_X, SPAWN_Y};

use shared::{ClientMessage, PlayerState, ServerMessage};
use std::collections::HashMap;
use tracing::{error, info, warn};

use crate::ServerConfig;

#[derive(Debug, Clone, PartialEq)]
pub enum EntityState {
    Owned,
    PendingHandoff { neighbor_topics: Vec<[u8; 32]> },
    Ghost,
}

pub struct PlayerData {
    pub username: String,
    pub position: Vec2,
    pub velocity: Vec2,
    pub state: EntityState,
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
    pub buffer: Vec<u8>,
}

#[derive(Resource, Default)]
pub struct NetworkFrameCounter {
    pub frame_count: u64,
}

#[derive(Resource, Default)]
pub struct NetworkDiagnostics {
    pub position_updates_attempted: u64,
    pub position_updates_failed: u64,
    pub aoi_broadcasts_sent: u64,
    pub aoi_broadcasts_failed: u64,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<PlayerRegistry>()
            .init_resource::<NetworkFrameCounter>()
            .init_resource::<NetworkDiagnostics>()
            .add_systems(
                Update,
                (
                    poll_network_events,
                    broadcast_positions,
                    broadcast_aoi,
                    network_frame_diagnostic,
                )
                    .chain(),
            );
    }
}

fn poll_network_events(
    mut net: ResMut<NetworkManager>,
    mut registry: ResMut<PlayerRegistry>,
    config: Res<ServerConfig>,
) {
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
                let topic = string_to_topic(&format!("shard:{}", config.id));
                let dummy_msg = BrokerMessage::Publish {
                    topic,
                    payload: vec![], // Empty payload
                };

                if stream.is_reliable() {
                    info!("[NETWORK] Reliable stream to Broker is ready. Registering shard.");

                    // Send Dummy message which contains the shard topic in order to register this connection to the shard topic
                    if let Err(e) =
                        net.peer
                            .send(&_connection, &stream, Bytes::from(dummy_msg.to_bytes()))
                    {
                        error!("Failed to send dummy publish: {:?}", e);
                    } else {
                        info!(
                            "[NETWORK] Successfully registered to topic: {}",
                            &format!("shard:{}", config.id)
                        );
                    }

                    let ready_msg = BrokerMessage::ShardReady {
                        shard_id: config.id,
                    };
                    let _ = net
                        .peer
                        .send(&_connection, &stream, Bytes::from(ready_msg.to_bytes()));

                    net.reliable_stream = Some(stream);
                } else {
                    info!("[NETWORK] Unreliable stream to Broker is ready. Waking it up.");
                    if let Err(e) =
                        net.peer
                            .send(&_connection, &stream, Bytes::from(dummy_msg.to_bytes()))
                    {
                        error!("Failed to send unreliable dummy publish: {:?}", e);
                    } else {
                        info!("[NETWORK] Unreliable stream to Broker is awake.");
                    }
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
                net.buffer.extend_from_slice(&data);

                // First, unwrap the Broker protocol envelope and parse all messages
                for broker_msg in BrokerMessage::parse_multiple(&mut net.buffer) {
                    handle_broker_message(broker_msg, &mut registry, &mut net, &config);
                }
            }
            _ => {}
        }
    }
}

// ----------------------------------------------------------------------------------------------------------------------------------------
// Read and handle messages from the Broker. For now : JOIN and MOVE_INPUT from clients, wrapped in the BrokerMessage::ClientInput variant.
// ----------------------------------------------------------------------------------------------------------------------------------------
fn handle_broker_message(
    msg: BrokerMessage,
    registry: &mut PlayerRegistry,
    net: &mut NetworkManager,
    config: &ServerConfig,
) {
    let my_topic = string_to_topic(&config.zone);

    match msg {
        // ==========================================
        // Client Inputs
        // ==========================================

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
                                position: Vec2::new(SPAWN_X, SPAWN_Y),
                                velocity: Vec2::new(0.0, 0.0),
                                state: EntityState::Owned,
                            },
                        );
                    }
                    ClientMessage::MoveInput { x, y } => {
                        if let Some(player) = registry.players.get_mut(&client_id) {
                            let speed = 5.0;
                            player.velocity = Vec2::new(x * speed, y * speed);

                            player.position.x = (player.position.x + player.velocity.x)
                                .clamp(MAP_BOUND_MIN, MAP_BOUND_MAX);
                            player.position.y = (player.position.y + player.velocity.y)
                                .clamp(MAP_BOUND_MIN, MAP_BOUND_MAX);

                            debug!(
                                "[GAME] Received input from player {} (ID: {}). New position: ({:.2}, {:.2})",
                                player.username, client_id, player.position.x, player.position.y
                            );
                        }
                        // Ignores the input if the player is not registered
                        // Can be when an entity is not already spawned with ghost state
                    }
                    ClientMessage::Disconnect => {
                        if let Some(player) = registry.players.remove(&client_id) {
                            info!(
                                "[GAME] Player {} (ID: {}) disconnected from the shard.",
                                player.username, client_id
                            );
                        } else {
                            info!(
                                "[GAME] Unknown player with ID: {} disconnected from the shard.",
                                client_id
                            );
                        }
                    }
                }
            }
        }

        // ==========================================
        // Spatial Server Messages
        // ==========================================
        BrokerMessage::CrossingAlert {
            client_id,
            dest_authority_topic,
            neighbor_topic,
        } => {
            if dest_authority_topic == my_topic {
                if let Some(player) = registry.players.get_mut(&client_id) {
                    info!(
                        "CROSSING ALERT: {} changes state to PendingHandoff",
                        client_id
                    );

                    // Switch state and add the new neighbor to the list of handoff targets
                    match &mut player.state {
                        EntityState::Owned => {
                            info!(
                                "CROSSING ALERT: {} switch to PendingHandoff for {:?}",
                                client_id, neighbor_topic
                            );
                            player.state = EntityState::PendingHandoff {
                                neighbor_topics: vec![neighbor_topic],
                            };
                        }
                        EntityState::PendingHandoff { neighbor_topics } => {
                            if !neighbor_topics.contains(&neighbor_topic) {
                                info!(
                                    "CROSSING ALERT: {} adds {:?} to its ghost targets",
                                    client_id, neighbor_topic
                                );
                                neighbor_topics.push(neighbor_topic);
                            }
                        }
                        EntityState::Ghost => {}
                    }

                    // Send HandoffRequest (0x20) to the new authority with the entity state for spawning the ghost
                    let mut state_buf = [0u8; 64];
                    let name_bytes = player.username.as_bytes();
                    let len = name_bytes.len().min(64);
                    state_buf[..len].copy_from_slice(&name_bytes[..len]);

                    let req = InterShardPayload::HandoffRequest {
                        entity_id: client_id,
                        pos_x: player.position.x,
                        pos_y: player.position.y,
                        vel_x: player.velocity.x,
                        vel_y: player.velocity.y,
                        state: state_buf,
                    };
                    send_inter_shard(net, my_topic, neighbor_topic, req);
                }
            }
        }

        BrokerMessage::AuthoritySwitch {
            client_id,
            old_auth_topic,
            new_auth_topic,
        } => {
            // AuthoritySwitch is received by the old authority only
            if old_auth_topic == my_topic {
                if let Some(player) = registry.players.get_mut(&client_id) {
                    info!("AUTHORITY SWITCH: {} changes state to Ghost", client_id);

                    let comp = InterShardPayload::HandoffComplete {
                        entity_id: client_id,
                    };
                    send_inter_shard(net, my_topic, new_auth_topic, comp);

                    player.state = EntityState::Ghost;
                }
            }
        }

        BrokerMessage::CrossingExit {
            client_id,
            obsolete_auth_topic,
            new_auth_topic,
        } => {
            if obsolete_auth_topic == my_topic {
                // This shard is obsoleted for this entity
                info!("CROSSING EXIT: Ghost removed {}", client_id);
                registry.players.remove(&client_id);
            } else if new_auth_topic == my_topic {
                // This shard is the new authority, the player has left the margin, it switches to Owned
                if let Some(player) = registry.players.get_mut(&client_id) {
                    if let EntityState::PendingHandoff { neighbor_topics } = &mut player.state {
                        neighbor_topics.retain(|&t| t != obsolete_auth_topic);

                        // Switches to Owned if there is no more neighbor to send ghost updates to
                        if neighbor_topics.is_empty() {
                            info!(
                                "CROSSING EXIT: {} no more pending handoff, switch to Owned",
                                client_id
                            );
                            player.state = EntityState::Owned;
                        } else {
                            info!(
                                "CROSSING EXIT: {} stays in PendingHandoff for {} other margin(s)",
                                client_id,
                                neighbor_topics.len()
                            );
                        }
                    }
                }
            }
        }

        // ==========================================
        // Inter-Shard Messages (for handoff)
        // ==========================================
        BrokerMessage::InterShardMessage {
            topic_dest,
            topic_from,
            payload,
        } => {
            if topic_dest == my_topic {
                if let Some(inter_msg) = InterShardPayload::from_bytes(&payload) {
                    match inter_msg {
                        InterShardPayload::HandoffRequest {
                            entity_id,
                            pos_x,
                            pos_y,
                            vel_x,
                            vel_y,
                            state,
                        } => {
                            // Spawn a ghost
                            let clean_username = String::from_utf8_lossy(&state)
                                .trim_end_matches('\0')
                                .to_string();
                            registry.players.insert(
                                entity_id,
                                PlayerData {
                                    username: clean_username,
                                    position: Vec2::new(pos_x, pos_y),
                                    velocity: Vec2::new(vel_x, vel_y),
                                    state: EntityState::Ghost,
                                },
                            );
                            info!("GHOST CREATED for the player {}", entity_id);
                        }
                        InterShardPayload::GhostUpdate {
                            entity_id,
                            pos_x,
                            pos_y,
                            vel_x,
                            vel_y,
                        } => {
                            // Apply correction to the ghost's position/velocity
                            if let Some(player) = registry.players.get_mut(&entity_id) {
                                if player.state == EntityState::Ghost {
                                    player.position = Vec2::new(pos_x, pos_y);
                                    player.velocity = Vec2::new(vel_x, vel_y);
                                }
                            }
                        }
                        InterShardPayload::HandoffComplete { entity_id } => {
                            // Receive authority over the entity, switch to PendingHandoff
                            if let Some(player) = registry.players.get_mut(&entity_id) {
                                info!(
                                    "HANDOFF COMPLETE Received: {} Switching to PendingHandoff",
                                    entity_id
                                );
                                player.state = EntityState::PendingHandoff {
                                    neighbor_topics: vec![topic_from],
                                };
                            }
                        }
                    }
                }
            }
        }

        _ => {}
    }
}

// -------------------------------------------------------------------------
// Helper function to send inter-shard messages via the Broker
// -------------------------------------------------------------------------
fn send_inter_shard(
    net: &mut NetworkManager,
    from: [u8; 32],
    to: [u8; 32],
    payload: InterShardPayload,
) {
    let Some(conn) = &net.broker_connection else {
        return;
    };
    let Some(stream) = &net.reliable_stream else {
        return;
    }; // TODO: choisir unreliable_stream si c'est un GhostUpdate

    let msg = BrokerMessage::InterShardMessage {
        topic_dest: to,
        topic_from: from,
        payload: payload.to_bytes(),
    };

    let _ = net.peer.send(conn, stream, Bytes::from(msg.to_bytes()));
}

// -------------------------------------------------------------------------
// Position Updates (20Hz) - For Spatial Server
// -------------------------------------------------------------------------
fn broadcast_positions(
    net: ResMut<NetworkManager>,
    registry: Res<PlayerRegistry>,
    config: Res<ServerConfig>,
    mut diagnostics: ResMut<NetworkDiagnostics>,
) {
    let Some(broker_conn) = &net.broker_connection else {
        // debug!("[NETWORK] Cannot send positions: broker_connection is None");
        return;
    };
    let Some(unrel_stream) = &net.unreliable_stream else {
        // debug!("[NETWORK] Cannot send positions: unreliable_stream is None");
        return;
    };

    let my_topic = string_to_topic(&config.zone);
    let mut positions_sent = 0usize;
    let mut positions_failed = 0usize;

    // ============ Send Position Updates for the Spatial Server (20Hz) ===========
    for (client_id, player_data) in &registry.players {
        if player_data.state == EntityState::Ghost {
            // Don't send PositionUpdates for ghosts
            continue;
        }

        // Notify the Spatial Server of the exact coordinates
        let pos_update = BrokerMessage::PositionUpdate {
            client_id: *client_id,
            x: player_data.position.x,
            y: player_data.position.y,
        };

        diagnostics.position_updates_attempted += 1;
        if let Err(e) = net.peer.send(
            broker_conn,
            unrel_stream,
            Bytes::from(pos_update.to_bytes()),
        ) {
            error!(
                "[NETWORK] Failed to send PositionUpdate for client {}: {:?}",
                client_id, e
            );
            diagnostics.position_updates_failed += 1;
            positions_failed += 1;
        } else {
            positions_sent += 1;
        }

        // --- Handoff Logics : Send GhostUpdates (20Hz) ---
        if let EntityState::PendingHandoff { neighbor_topics } = &player_data.state {
            let ghost_update_payload = InterShardPayload::GhostUpdate {
                entity_id: *client_id,
                pos_x: player_data.position.x,
                pos_y: player_data.position.y,
                vel_x: player_data.velocity.x,
                vel_y: player_data.velocity.y,
            }
            .to_bytes();

            for neighbor_topic in neighbor_topics {
                let msg = BrokerMessage::InterShardMessage {
                    topic_dest: *neighbor_topic,
                    topic_from: my_topic,
                    payload: ghost_update_payload.clone(),
                };
                if let Err(e) =
                    net.peer
                        .send(broker_conn, unrel_stream, Bytes::from(msg.to_bytes()))
                {
                    warn!("[NETWORK] Failed to send GhostUpdate to neighbor: {:?}", e);
                }
            }
        }
    }

    if positions_sent > 0 || positions_failed > 0 {
        debug!(
            "[NETWORK-20Hz] PositionUpdates: {} sent, {} failed",
            positions_sent, positions_failed
        );
    }
}

// -------------------------------------------------------------------------
// AOI Broadcast (20Hz) - For Clients
// -------------------------------------------------------------------------
fn broadcast_aoi(
    net: ResMut<NetworkManager>,
    registry: Res<PlayerRegistry>,
    config: Res<ServerConfig>,
    mut diagnostics: ResMut<NetworkDiagnostics>,
) {
    let Some(broker_conn) = &net.broker_connection else {
        debug!("[NETWORK] Cannot broadcast AOI: broker_connection is None");
        return;
    };
    let Some(unrel_stream) = &net.unreliable_stream else {
        debug!("[NETWORK] Cannot broadcast AOI: unreliable_stream is None");
        return;
    };

    let mut all_players = Vec::new();

    // ============ Collect all non-ghost players for AOI snapshot ===========
    for (client_id, player_data) in &registry.players {
        if player_data.state == EntityState::Ghost {
            continue;
        }

        all_players.push(PlayerState {
            id: *client_id,
            username: player_data.username.clone(),
            x: player_data.position.x,
            y: player_data.position.y,
        });
    }

    if all_players.is_empty() {
        return;
    }

    // ============ Publish the Global AOI of this Shard (20Hz) ===========
    let player_count = all_players.len();
    let snapshot = ServerMessage::AOISnapshot {
        players: all_players,
    };

    match bincode::serialize(&snapshot) {
        Ok(payload) => {
            let publish_msg = BrokerMessage::Publish {
                topic: string_to_topic(&config.zone),
                payload,
            };

            diagnostics.aoi_broadcasts_sent += 1;
            if let Err(e) = net.peer.send(
                broker_conn,
                unrel_stream,
                Bytes::from(publish_msg.to_bytes()),
            ) {
                warn!("[NETWORK] Failed to publish AOI to Broker: {:?}", e);
                diagnostics.aoi_broadcasts_failed += 1;
            } else {
                debug!(
                    "[NETWORK-20Hz] AOI snapshot published: {} players",
                    player_count
                );
            }
        }
        Err(e) => error!("Failed to serialize AOI Snapshot: {:?}", e),
    }
}

// -------------------------------------------------------------------------
// Frame Rate Diagnostic - logs every 100 frames to verify 20Hz operation
// -------------------------------------------------------------------------
fn network_frame_diagnostic(mut frame_counter: ResMut<NetworkFrameCounter>) {
    frame_counter.frame_count += 1;
    // Commented out to reduce console pollution - uncomment for debugging frame rates
    // if frame_counter.frame_count % 20 == 0 {
    //     debug!(
    //         "[NETWORK] {} frames processed (20Hz = 1 frame every 50ms)",
    //         frame_counter.frame_count
    //     );
    // }
}
