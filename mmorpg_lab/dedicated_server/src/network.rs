use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability};
use shared::broker_protocol::{BrokerMessage, InterShardPayload, string_to_topic};
use shared::{MAP_BOUND_MAX, MAP_BOUND_MIN, SPAWN_X, SPAWN_Y};

use std::collections::HashSet;

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
    pub score: f32,
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
    pub reliable_buffer: Vec<u8>,
    pub unreliable_buffer: Vec<u8>,
}

#[derive(Resource, Default)]
pub struct NetworkFrameCounter {
    pub frame_count: u64,
}

#[derive(Resource, Default)]
pub struct NetworkDiagnostics {
    pub position_updates_attempted: u64,
    //pub position_updates_failed: u64,
    pub aoi_broadcasts_sent: u64,
    //pub aoi_broadcasts_failed: u64,
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
                    process_collisions,
                    broadcast_network_updates,
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
    mut food_registry: ResMut<crate::food::FoodRegistry>,
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

                    // Ask the parent shard to send some food
                    if config.parent_shard_id != config.id {
                        let parent_id = config.parent_shard_id;
                        info!(
                            "[NETWORK] Requesting food takeover from parent shard: {}",
                            parent_id
                        );
                        let req = InterShardPayload::FoodTakeoverRequest {
                            bounds_x: config.bounds.x,
                            bounds_y: config.bounds.y,
                            bounds_w: config.bounds.width,
                            bounds_h: config.bounds.height,
                        };
                        let msg = BrokerMessage::InterShardMessage {
                            topic_dest: string_to_topic(&format!("shard:{}", parent_id)),
                            topic_from: string_to_topic(&config.zone),
                            payload: req.to_bytes(),
                        };
                        let _ = net
                            .peer
                            .send(&_connection, &stream, Bytes::from(msg.to_bytes()));
                    }

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
            GameNetworkEvent::Message { stream, data, .. } => {
                // First, unwrap the Broker protocol envelope and parse all messages
                let buffer = if stream.is_reliable() {
                    &mut net.reliable_buffer
                } else {
                    &mut net.unreliable_buffer
                };

                buffer.extend_from_slice(&data);

                for broker_msg in BrokerMessage::parse_multiple(buffer) {
                    handle_broker_message(
                        broker_msg,
                        &mut registry,
                        &mut net,
                        &config,
                        &mut food_registry,
                    );
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
    food_registry: &mut crate::food::FoodRegistry,
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
                                score: 0.0,
                            },
                        );

                        // Send current food data to the new player
                        if !(net.broker_connection.is_none() || net.reliable_stream.is_none()) {
                            let all_food: Vec<shared::FoodData> =
                                food_registry.food.values().cloned().collect();
                            if !all_food.is_empty() {
                                if let Ok(payload) =
                                    bincode::serialize(&ServerMessage::FoodSync(all_food))
                                {
                                    let food_msg =
                                        BrokerMessage::DirectMessageReliable { client_id, payload }
                                            .to_bytes();

                                    if let (Some(conn), Some(stream)) =
                                        (&net.broker_connection, &net.reliable_stream)
                                    {
                                        let _ = net.peer.send(conn, stream, Bytes::from(food_msg));
                                    }
                                }
                            }
                        } else {
                            warn!(
                                "Cannot send food data to new player {}: no connection to broker or reliable stream",
                                client_id
                            );
                        }
                    }
                    ClientMessage::MoveInput { x, y } => {
                        if let Some(player) = registry.players.get_mut(&client_id) {
                            let base_speed = 5.0;
                            let current_speed =
                                (base_speed / (1.0 + (player.score * 0.005))).max(1.0);

                            player.velocity = Vec2::new(x * current_speed, y * current_speed);

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
                        score: player.score,
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
                            score,
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
                                    score: score,
                                },
                            );
                            info!("GHOST CREATED for the player {}", entity_id);

                            // Send reliable message to the new ghost with all the current food data in order to spawn them correctly
                            let all_food: Vec<shared::FoodData> =
                                food_registry.food.values().cloned().collect();
                            if !all_food.is_empty() {
                                if let Ok(payload) =
                                    bincode::serialize(&ServerMessage::FoodSync(all_food))
                                {
                                    let food_msg = BrokerMessage::DirectMessageReliable {
                                        client_id: entity_id,
                                        payload,
                                    }
                                    .to_bytes();

                                    if let (Some(conn), Some(stream)) =
                                        (&net.broker_connection, &net.reliable_stream)
                                    {
                                        let _ = net.peer.send(conn, stream, Bytes::from(food_msg));
                                    }
                                }
                            }
                        }
                        InterShardPayload::GhostUpdate {
                            entity_id,
                            pos_x,
                            pos_y,
                            vel_x,
                            vel_y,
                            score,
                        } => {
                            // Apply correction to the ghost's position/velocity
                            if let Some(player) = registry.players.get_mut(&entity_id) {
                                if player.state == EntityState::Ghost {
                                    player.position = Vec2::new(pos_x, pos_y);
                                    player.velocity = Vec2::new(vel_x, vel_y);
                                    player.score = score;
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
                        InterShardPayload::FoodTakeoverRequest {
                            bounds_x,
                            bounds_y,
                            bounds_w,
                            bounds_h,
                        } => {
                            let rect = shared::rect::Rect {
                                x: bounds_x,
                                y: bounds_y,
                                width: bounds_w,
                                height: bounds_h,
                            };
                            let mut transfer_list = Vec::new();
                            let mut ids_to_remove = Vec::new();

                            for (&id, food) in food_registry.food.iter() {
                                let pos = shared::rect::Vec2 {
                                    x: food.x,
                                    y: food.y,
                                };
                                if rect.contains(&pos) {
                                    transfer_list.push(food.clone());
                                    ids_to_remove.push(id);
                                }
                            }

                            // Parent removes food from its registry without notifying clients
                            for id in &ids_to_remove {
                                food_registry.food.remove(id);
                                food_registry.ordered_ids.retain(|i| i != id);
                            }

                            info!(
                                "Transferring {} food items to new child shard",
                                transfer_list.len()
                            );

                            // Send payload to the new shard
                            if let Ok(bincode_payload) = bincode::serialize(&transfer_list) {
                                let resp = InterShardPayload::FoodTakeoverResponse {
                                    payload: bincode_payload,
                                };
                                send_inter_shard(net, my_topic, topic_from, resp);
                            }
                        }

                        InterShardPayload::FoodTakeoverResponse { payload } => {
                            if let Ok(food_list) =
                                bincode::deserialize::<Vec<shared::FoodData>>(&payload)
                            {
                                for food in food_list {
                                    food_registry.food.insert(food.id, food.clone());
                                    food_registry.ordered_ids.push(food.id);
                                }
                                info!(
                                    "Successfully inherited {} food items from parent!",
                                    food_registry.food.len()
                                );
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

fn broadcast_network_updates(
    net: ResMut<NetworkManager>,
    player_registry: Res<PlayerRegistry>,
    config: Res<ServerConfig>,
    mut diagnostics: ResMut<NetworkDiagnostics>,
    mut spawn_events: MessageReader<crate::food::FoodSpawnedMessage>,
    mut eaten_events: MessageReader<crate::food::FoodEatenMessage>,
    mut local_frame_count: Local<u64>,
) {
    *local_frame_count += 1;

    let Some(broker_conn) = &net.broker_connection else {
        debug!("[NETWORK] Cannot send positions: broker_connection is None");
        return;
    };
    let Some(unrel_stream) = &net.unreliable_stream else {
        debug!("[NETWORK] Cannot send positions: unreliable_stream is None");
        return;
    };
    let Some(rel_stream) = &net.reliable_stream else {
        debug!("[NETWORK] Cannot send positions: reliable_stream is None");
        return;
    };
    let my_topic = string_to_topic(&config.zone);

    // Message Buffer
    let mut batched_payload = Vec::new();
    let mut all_players = Vec::new();

    // Flush the buffer if it exceeds a certain size to avoid MTU issues
    let flush_buffer = |buffer: &mut Vec<u8>| {
        if !buffer.is_empty() {
            // info!("[NETWORK] Flushing buffer with {} bytes", buffer.len());
            let _ = net
                .peer
                .send(broker_conn, unrel_stream, Bytes::from(buffer.clone()));
            buffer.clear();
        }
    };

    // ============ Send client Position Updates for the Spatial Server (20Hz) And Ghost Updates to other shards ===========
    for (client_id, player_data) in &player_registry.players {
        if player_data.state == EntityState::Ghost {
            continue;
        }

        all_players.push(PlayerState {
            id: *client_id,
            username: player_data.username.clone(),
            x: player_data.position.x,
            y: player_data.position.y,
            score: player_data.score,
        });

        let pos_update = BrokerMessage::PositionUpdate {
            client_id: *client_id,
            x: player_data.position.x,
            y: player_data.position.y,
            score: player_data.score,
        };

        // MTU consideration : if adding this position update would exceed the MTU limit, flush the buffer first
        let pos_bytes = pos_update.to_bytes();
        if batched_payload.len() + pos_bytes.len() > 1000 {
            flush_buffer(&mut batched_payload);
        }
        batched_payload.extend_from_slice(&pos_bytes);
        diagnostics.position_updates_attempted += 1;

        // --- Handoff Logics : Send GhostUpdates (20Hz) ---
        if let EntityState::PendingHandoff { neighbor_topics } = &player_data.state {
            let payload = InterShardPayload::GhostUpdate {
                entity_id: *client_id,
                pos_x: player_data.position.x,
                pos_y: player_data.position.y,
                vel_x: player_data.velocity.x,
                vel_y: player_data.velocity.y,
                score: player_data.score,
            }
            .to_bytes();

            for neighbor_topic in neighbor_topics {
                let msg = BrokerMessage::InterShardMessage {
                    topic_dest: *neighbor_topic,
                    topic_from: my_topic,
                    payload: payload.clone(),
                };

                let ghost_bytes = msg.to_bytes();
                if batched_payload.len() + ghost_bytes.len() > 1000 {
                    flush_buffer(&mut batched_payload);
                }
                batched_payload.extend_from_slice(&ghost_bytes);
            }
        }
    }

    // Global AOI Snapshot for Clients (20Hz)
    if !all_players.is_empty() {
        // Make chunks of 15 players to avoid MTU issues with the AOI snapshot
        // Currently a player data is around 40 bytes, so 15 players is around 600 bytes, leaving room for the BrokerMessage envelope and some margin
        // If the player data size increases, we can reduce the chunk size to avoid MTU issues, or implement a more sophisticated batching mechanism
        for chunk in all_players.chunks(15) {
            let snapshot = ServerMessage::AOISnapshot {
                players: chunk.to_vec(),
            };

            //debug!( "[NETWORK] Broadcasting AOI Snapshot with {} players", chunk.len());

            if let Ok(aoi_bytes) = bincode::serialize(&snapshot) {
                let msg_bytes = BrokerMessage::Publish {
                    topic: my_topic,
                    payload: aoi_bytes,
                }
                .to_bytes();

                // Flush the buffer if adding this AOI snapshot would exceed the MTU limit
                if batched_payload.len() + msg_bytes.len() > 1000 {
                    flush_buffer(&mut batched_payload);
                }
                batched_payload.extend_from_slice(&msg_bytes);
                diagnostics.aoi_broadcasts_sent += 1;
            }
        }
    }

    // ============ Send Food Updates to clients (reliable event driven) ===========
    let eaten: Vec<u32> = eaten_events.read().map(|e| e.0).collect();
    if !eaten.is_empty() {
        let payload = bincode::serialize(&ServerMessage::FoodEaten(eaten)).unwrap();
        let bytes = BrokerMessage::PublishReliable {
            topic: my_topic,
            payload,
        }
        .to_bytes();
        let _ = net.peer.send(broker_conn, rel_stream, Bytes::from(bytes));
    }

    let spawned: Vec<shared::FoodData> = spawn_events.read().map(|e| e.0.clone()).collect();
    if !spawned.is_empty() {
        let payload = bincode::serialize(&ServerMessage::FoodSync(spawned)).unwrap();
        let bytes = BrokerMessage::PublishReliable {
            topic: my_topic,
            payload,
        }
        .to_bytes();
        let _ = net.peer.send(broker_conn, rel_stream, Bytes::from(bytes));
    }

    /*
    // Background Sync of Food Data (every tick, with rotation if there is a lot of food)
    // this is in addition to the immediate sync on spawn/despawn events, to ensure clients
    //  eventually receive all food data even if they miss the event messages (late join or packet loss)
    let total_food = food_registry.ordered_ids.len();

    if total_food > 0 {
        let cycle_frames = 100; // Number of frames to complete a full sync cycle
        let current_slice_index = (*local_frame_count % cycle_frames) as usize;

        let slice_size = (total_food / cycle_frames as usize).max(1);

        let start_idx = current_slice_index * slice_size;
        let mut end_idx = start_idx + slice_size;

        if current_slice_index == cycle_frames as usize - 1 {
            end_idx = total_food;
        } else {
            end_idx = end_idx.min(total_food);
        }

        if start_idx < total_food {
            let slice_ids = &food_registry.ordered_ids[start_idx..end_idx];

            let mut my_slice = Vec::with_capacity(slice_ids.len());
            for id in slice_ids {
                if let Some(food_data) = food_registry.food.get(id) {
                    my_slice.push(food_data.clone());
                }
            }

            if !my_slice.is_empty() {
                let payload = bincode::serialize(&ServerMessage::FoodSync(my_slice)).unwrap();
                let bytes = BrokerMessage::Publish {
                    topic: my_topic,
                    payload,
                }
                .to_bytes();

                if batched_payload.len() + bytes.len() > 1000 {
                    flush_buffer(&mut batched_payload);
                }
                batched_payload.extend_from_slice(&bytes);
            }
        }
    }
    */

    // Final flush after processing all updates
    flush_buffer(&mut batched_payload);
}

// -------------------------------------------------------------------------
// Process collisions between players and food, and between players themselves. Update scores, remove eaten food, and handle player deaths.
// --------------------------------------------------------------------------
pub fn process_collisions(
    mut registry: ResMut<PlayerRegistry>,
    mut food_registry: ResMut<crate::food::FoodRegistry>,
    mut eaten_writer: MessageWriter<crate::food::FoodEatenMessage>,
    net: ResMut<NetworkManager>,
) {
    let mut eaten_food = HashSet::new();
    let mut players_to_kill = HashSet::new();
    let mut score_gains = HashMap::new();

    // ==========================================
    // Collision with food
    // ==========================================
    for (client_id, player) in registry.players.iter() {
        // Ghosts cannot eat food in the shard
        if player.state == EntityState::Ghost {
            continue;
        }

        let player_radius = 15.0 + player.score;
        let player_pos = player.position;

        for (&food_id, food) in food_registry.food.iter() {
            if eaten_food.contains(&food_id) {
                continue;
            }

            let food_pos = Vec2::new(food.x, food.y);
            if player_pos.distance(food_pos) < player_radius {
                eaten_food.insert(food_id);
                *score_gains.entry(*client_id).or_insert(0.0) += 0.5; // 1 point par bille
            }
        }
    }

    // ==========================================
    // Collision between players
    // ==========================================
    let player_ids: Vec<u32> = registry.players.keys().copied().collect();
    for i in 0..player_ids.len() {
        for j in (i + 1)..player_ids.len() {
            let id_a = player_ids[i];
            let id_b = player_ids[j];

            let p_a = registry.players.get(&id_a).unwrap();
            let p_b = registry.players.get(&id_b).unwrap();

            let radius_a = 15.0 + p_a.score;
            let radius_b = 15.0 + p_b.score;
            let dist = p_a.position.distance(p_b.position);

            // players are overlapping each other
            if dist < radius_a || dist < radius_b {
                // A eats B since he is overlapping B and is at least 10% bigger
                if radius_a >= radius_b * 1.10 && dist < radius_a {
                    if !players_to_kill.contains(&id_b) {
                        players_to_kill.insert(id_b);

                        // owning shard attributes score
                        if !(p_a.state == EntityState::Ghost) {
                            *score_gains.entry(id_a).or_insert(0.0) += p_b.score * 0.5; // Take 50% of the eaten player's score as gain
                        }
                    }
                }
                // B eats A
                else if radius_b >= radius_a * 1.10 && dist < radius_b {
                    if !players_to_kill.contains(&id_a) {
                        players_to_kill.insert(id_a);
                        if !(p_b.state == EntityState::Ghost) {
                            *score_gains.entry(id_b).or_insert(0.0) += p_a.score * 0.5;
                        }
                    }
                }
            }
        }
    }

    // ==========================================
    // Apply results of collisions
    // ==========================================

    // Add scores
    for (id, gain) in score_gains {
        if let Some(p) = registry.players.get_mut(&id) {
            p.score += gain;
        }
    }

    // destroy eaten food and notify clients
    for food_id in eaten_food {
        if food_registry.food.remove(&food_id).is_some() {
            food_registry.ordered_ids.retain(|&id| id != food_id);
            eaten_writer.write(crate::food::FoodEatenMessage(food_id));
        }
    }

    // Player's deaths - remove them from the registry and notify clients
    for id in players_to_kill {
        if let Some(dead_player) = registry.players.remove(&id) {
            if !(dead_player.state == EntityState::Ghost) {
                info!("Player {} has been eaten!", dead_player.username);

                // Send GameOver message to the eaten client
                if let (Some(conn), Some(stream)) = (&net.broker_connection, &net.reliable_stream) {
                    if let Ok(payload) = bincode::serialize(&shared::ServerMessage::GameOver) {
                        let msg = shared::broker_protocol::BrokerMessage::DirectMessageReliable {
                            client_id: id,
                            payload,
                        }
                        .to_bytes();
                        let _ = net.peer.send(conn, stream, Bytes::from(msg));
                    }
                }
            } else {
                info!(
                    "Ghost of {} has been eaten! The master server will handle it.",
                    dead_player.username
                );
            }
        }
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

// // ===================================================================================================================
// // The following functions should probably be removed if broadcast_network_updates works properly, they are left here for reference and debugging purposes.
// // ===================================================================================================================

// // -------------------------------------------------------------------------
// // Position Updates (20Hz) - For Spatial Server
// // -------------------------------------------------------------------------
// fn broadcast_positions(
//     net: ResMut<NetworkManager>,
//     registry: Res<PlayerRegistry>,
//     config: Res<ServerConfig>,
//     mut diagnostics: ResMut<NetworkDiagnostics>,
// ) {
//     let Some(broker_conn) = &net.broker_connection else {
//         debug!("[NETWORK] Cannot send positions: broker_connection is None");
//         return;
//     };
//     let Some(unrel_stream) = &net.unreliable_stream else {
//         debug!("[NETWORK] Cannot send positions: unreliable_stream is None");
//         return;
//     };

//     let my_topic = string_to_topic(&config.zone);
//     let mut positions_sent = 0usize;
//     let mut positions_failed = 0usize;

//     // ============ Send Position Updates for the Spatial Server (20Hz) ===========
//     for (client_id, player_data) in &registry.players {
//         if player_data.state == EntityState::Ghost {
//             // Don't send PositionUpdates for ghosts
//             continue;
//         }

//         // Notify the Spatial Server of the exact coordinates
//         let pos_update = BrokerMessage::PositionUpdate {
//             client_id: *client_id,
//             x: player_data.position.x,
//             y: player_data.position.y,
//         };

//         diagnostics.position_updates_attempted += 1;
//         if let Err(e) = net.peer.send(
//             broker_conn,
//             unrel_stream,
//             Bytes::from(pos_update.to_bytes()),
//         ) {
//             error!(
//                 "[NETWORK] Failed to send PositionUpdate for client {}: {:?}",
//                 client_id, e
//             );
//             diagnostics.position_updates_failed += 1;
//             positions_failed += 1;
//         } else {
//             positions_sent += 1;
//         }

//         // --- Handoff Logics : Send GhostUpdates (20Hz) ---
//         if let EntityState::PendingHandoff { neighbor_topics } = &player_data.state {
//             let ghost_update_payload = InterShardPayload::GhostUpdate {
//                 entity_id: *client_id,
//                 pos_x: player_data.position.x,
//                 pos_y: player_data.position.y,
//                 vel_x: player_data.velocity.x,
//                 vel_y: player_data.velocity.y,
//             }
//             .to_bytes();

//             for neighbor_topic in neighbor_topics {
//                 let msg = BrokerMessage::InterShardMessage {
//                     topic_dest: *neighbor_topic,
//                     topic_from: my_topic,
//                     payload: ghost_update_payload.clone(),
//                 };
//                 if let Err(e) =
//                     net.peer
//                         .send(broker_conn, unrel_stream, Bytes::from(msg.to_bytes()))
//                 {
//                     warn!("[NETWORK] Failed to send GhostUpdate to neighbor: {:?}", e);
//                 }
//             }
//         }
//     }

//     if positions_sent > 0 || positions_failed > 0 {
//         debug!(
//             "[NETWORK-20Hz] PositionUpdates: {} sent, {} failed",
//             positions_sent, positions_failed
//         );
//     }
// }

// // -------------------------------------------------------------------------
// // AOI Broadcast (20Hz) - For Clients
// // -------------------------------------------------------------------------
// fn broadcast_aoi(
//     net: ResMut<NetworkManager>,
//     registry: Res<PlayerRegistry>,
//     config: Res<ServerConfig>,
//     mut diagnostics: ResMut<NetworkDiagnostics>,
// ) {
//     let Some(broker_conn) = &net.broker_connection else {
//         debug!("[NETWORK] Cannot broadcast AOI: broker_connection is None");
//         return;
//     };
//     let Some(unrel_stream) = &net.unreliable_stream else {
//         debug!("[NETWORK] Cannot broadcast AOI: unreliable_stream is None");
//         return;
//     };

//     let mut all_players = Vec::new();

//     // ============ Collect all non-ghost players for AOI snapshot ===========
//     for (client_id, player_data) in &registry.players {
//         if player_data.state == EntityState::Ghost {
//             continue;
//         }

//         all_players.push(PlayerState {
//             id: *client_id,
//             username: player_data.username.clone(),
//             x: player_data.position.x,
//             y: player_data.position.y,
//         });
//     }

//     if all_players.is_empty() {
//         return;
//     }

//     // ============ Publish the Global AOI of this Shard (20Hz) ===========
//     let player_count = all_players.len();
//     let snapshot = ServerMessage::AOISnapshot {
//         players: all_players,
//     };

//     match bincode::serialize(&snapshot) {
//         Ok(payload) => {
//             let publish_msg = BrokerMessage::Publish {
//                 topic: string_to_topic(&config.zone),
//                 payload,
//             };

//             diagnostics.aoi_broadcasts_sent += 1;
//             if let Err(e) = net.peer.send(
//                 broker_conn,
//                 unrel_stream,
//                 Bytes::from(publish_msg.to_bytes()),
//             ) {
//                 warn!("[NETWORK] Failed to publish AOI to Broker: {:?}", e);
//                 diagnostics.aoi_broadcasts_failed += 1;
//             } else {
//                 debug!(
//                     "[NETWORK-20Hz] AOI snapshot published: {} players",
//                     player_count
//                 );
//             }
//         }
//         Err(e) => error!("Failed to serialize AOI Snapshot: {:?}", e),
//     }
// }
