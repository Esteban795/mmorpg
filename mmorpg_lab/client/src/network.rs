use bevy::prelude::*;
use bytes::Bytes;
use game_sockets::{
    GameConnection, GameNetworkEvent, GamePeer, GameStream, GameStreamReliability,
    protocols::QuicBackend,
};
use shared::broker_protocol::BrokerMessage;
use shared::{ClientMessage, ServerMessage};

use crate::chatbox::ChatState;
use crate::game::{TargetTransform, spawn_food, spawn_player};
use crate::loginmenu::ConnectionSettings;
use crate::state::AppState;

use tracing::{error, info};

#[derive(Resource, Default)]
pub struct ClientNetworkManager {
    pub peer: Option<GamePeer>,
    pub server_connection: Option<GameConnection>,
    pub reliable_stream: Option<GameStream>,
    pub unreliable_stream: Option<GameStream>,
    pub reliable_buffer: Vec<u8>,
    pub unreliable_buffer: Vec<u8>,
}

pub struct NetworkPlugin;

impl Plugin for NetworkPlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<ClientNetworkManager>()
            .add_systems(OnEnter(AppState::InGame), start_connection)
            .add_systems(Update, handle_network.run_if(in_state(AppState::InGame)));
    }
}

// Creates game_socket connection to the given server (in fact to the broker)
fn start_connection(
    mut net: ResMut<ClientNetworkManager>,
    mut settings: ResMut<ConnectionSettings>,
    mut next_state: ResMut<NextState<AppState>>,
) {
    if let Some((ip, port)) = &settings.server_target {
        info!("[CLIENT] : Starting connection to {}:{}...", ip, port);

        let backend = QuicBackend::new();
        let peer = GamePeer::new(backend);

        if let Err(e) = peer.connect(ip, *port) {
            error!("[CLIENT] ERROR: Failed to connect: {:?}", e);
            settings.error_message = Some("Failed to connect to server".to_string());
            next_state.set(AppState::LoginMenu);
            return;
        }

        net.peer = Some(peer);
    }
}

// This system continuously polls the network for events from the server. It handles connection events, stream creation, incoming messages, and disconnections.
fn handle_network(
    mut net: ResMut<ClientNetworkManager>,
    settings: Res<ConnectionSettings>,
    mut commands: Commands,
    mut game_state: ResMut<crate::game::GameState>,
    mut targets: Query<&mut TargetTransform>,
    time: Res<Time>,
    mut chat_state: ResMut<ChatState>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<ColorMaterial>>,
    mut next_state: ResMut<NextState<AppState>>,
) {
    let current_time = time.elapsed_secs_f64();

    loop {
        let event_result = if let Some(peer) = &mut net.peer {
            peer.poll()
        } else {
            return; // If peer isn't intialized yet, skip the system
        };

        match event_result {
            Ok(Some(event)) => match event {
                GameNetworkEvent::Connected(conn) => {
                    info!("[CLIENT]: Connected to server !");
                    net.server_connection = Some(conn);
                    if let Some(peer) = &mut net.peer {
                        peer.create_stream(conn, GameStreamReliability::Reliable)
                            .unwrap();
                        peer.create_stream(conn, GameStreamReliability::Unreliable)
                            .unwrap();
                    }
                }
                GameNetworkEvent::StreamCreated(conn, stream) => {
                    handle_stream_created(conn, stream, &mut net, &settings);
                }
                GameNetworkEvent::Message { stream, data, .. } => {
                    let buffer = if stream.is_reliable() {
                        &mut net.reliable_buffer
                    } else {
                        &mut net.unreliable_buffer
                    };

                    buffer.extend_from_slice(&data);

                    for broker_msg in shared::broker_protocol::BrokerMessage::parse_multiple(buffer)
                    {
                        handle_server_message(
                            broker_msg,
                            &mut commands,
                            &mut game_state,
                            &mut targets,
                            current_time,
                            &mut net,
                            &mut chat_state,
                            &mut meshes,
                            &mut materials,
                            &mut next_state,
                        );
                    }
                }
                GameNetworkEvent::Disconnected(_) => {
                    info!("[CLIENT] : Disconnected from the server.");
                }
                _ => {}
            },
            Ok(None) | Err(_) => break, // No more event to process, exit the loop and wait for the next frame
        }
    }

    // Despawn of entities for players we haven't seen for a while:
    // If an entity in the HashMap hasn't been seen for more than 0.5 seconds (any AOI received), it is despawned and removed from the HashMap
    game_state
        .spawned_players
        .retain(|_id, (entity, last_seen)| {
            if current_time - *last_seen > 0.5 {
                commands.entity(*entity).despawn();
                false
            } else {
                true
            }
        });
}

// This function handles the creation of new streams from the broker. It distinguishes between reliable and unreliable streams,
fn handle_stream_created(
    conn: GameConnection,
    stream: GameStream,
    net: &mut ClientNetworkManager,
    settings: &ConnectionSettings,
) {
    if stream.is_reliable() {
        net.reliable_stream = Some(stream.clone());
        info!(
            "[CLIENT]: Sending join message with username: {}",
            settings.username
        );

        let mut username_bytes = [0u8; 12];
        let name_bytes = settings.username.as_bytes();
        let len = name_bytes.len().min(12);
        username_bytes[..len].copy_from_slice(&name_bytes[..len]);

        let msg = ClientMessage::Join {
            username: username_bytes,
        };
        match bincode::serialize(&msg) {
            Ok(bytes) => {
                // Send the join message to the server via the reliable stream
                let mut input_array = [0u8; 16];
                let len = bytes.len().min(16);
                input_array[..len].copy_from_slice(&bytes[..len]);

                let broker_msg = BrokerMessage::ClientInput {
                    client_id: 0, // TODO: wait for the broker's welcome message to get the actual client ID before sending inputs
                    input: input_array,
                };

                if let Some(peer) = &mut net.peer {
                    let _ = peer.send(&conn, &stream, Bytes::from(broker_msg.to_bytes()));
                }
            }
            Err(e) => error!("Error serializing join message : {:?}", e),
        }
    } else {
        net.unreliable_stream = Some(stream.clone());

        // Send fake input to wake up the stream and make sure it's ready for low-latency messages
        //(and show all players in the AOI right after login, without waiting for the first real input from the player)
        // TODO: This move input is most likely to be dropped by the server since the client ID is not known yet, but it serves the purpose of waking up the stream.
        let wake_up_msg = ClientMessage::MoveInput { x: 0.0, y: 0.0 };
        match bincode::serialize(&wake_up_msg) {
            Ok(bytes) => {
                // Send the first MoveInput message to the server via the reliable stream
                let mut input_array = [0u8; 16];
                let len = bytes.len().min(16);
                input_array[..len].copy_from_slice(&bytes[..len]);

                let broker_msg = BrokerMessage::ClientInput {
                    client_id: 0,
                    input: input_array,
                };

                if let Some(peer) = &mut net.peer {
                    let _ = peer.send(&conn, &stream, Bytes::from(broker_msg.to_bytes()));
                }
            }
            Err(e) => error!("Error serializing MoveInput message : {:?}", e),
        }
    }
}

// This function processes messages received from the server. It deserializes the message and takes appropriate
// actions based on its type (e.g., handling welcome messages or AOI snapshots).
fn handle_server_message(
    msg: BrokerMessage,
    commands: &mut Commands,
    game_state: &mut crate::game::GameState,
    targets: &mut Query<&mut TargetTransform>,
    current_time: f64,
    net: &mut ClientNetworkManager,
    chat_state: &mut ChatState,
    meshes: &mut ResMut<Assets<Mesh>>,
    materials: &mut ResMut<Assets<ColorMaterial>>,
    next_state: &mut ResMut<NextState<AppState>>,
) {
    match msg {
        BrokerMessage::Broadcast { payload } => {
            if let Ok(server_msg) = bincode::deserialize::<ServerMessage>(&payload) {
                match server_msg {
                    ServerMessage::Welcome { player_id } => {
                        info!("[CLIENT]: Welcome ! My ID is: {}", player_id);
                        game_state.my_id = Some(player_id);

                        // Send fake input to wake up the stream and make sure it's ready for low-latency messages
                        //(and show all players in the AOI right after login, without waiting for the first real input from the player)
                        if let (Some(peer), Some(conn), Some(stream)) = (
                            &mut net.peer,
                            &net.server_connection,
                            &net.unreliable_stream,
                        ) {
                            let wake_up_msg = ClientMessage::MoveInput { x: 0.0, y: 0.0 };

                            if let Ok(bytes) = bincode::serialize(&wake_up_msg) {
                                let mut input_array = [0u8; 16];
                                let len = bytes.len().min(16);
                                input_array[..len].copy_from_slice(&bytes[..len]);

                                let broker_msg = BrokerMessage::ClientInput {
                                    client_id: player_id,
                                    input: input_array,
                                };

                                if let Err(e) = peer.send(
                                    conn,
                                    stream,
                                    bytes::Bytes::from(broker_msg.to_bytes()),
                                ) {
                                    error!("[CLIENT] Failed to send wake-up message: {:?}", e);
                                } else {
                                    info!("[CLIENT] Wake-up message sent successfully.");
                                }
                            }
                        } else {
                            error!(
                                "[CLIENT] Cannot send wake-up message: peer, connection or stream not ready."
                            );
                        }
                    }
                    ServerMessage::AOISnapshot { players } => {
                        crate::network::handle_aoi_snapshot(
                            players,
                            commands,
                            game_state,
                            targets,
                            current_time,
                            meshes,
                            materials,
                        );
                    }
                    ServerMessage::FoodSync(food_list) => {
                        //debug!("[CLIENT] FoodSync received: {} items", food_list.len());

                        for f in food_list {
                            spawn_food(commands, meshes, materials, game_state, &f, current_time);
                        }
                    }
                    ServerMessage::FoodEaten(eaten_ids) => {
                        info!(
                            "[CLIENT] FoodEaten received: {} items destroyed",
                            eaten_ids.len()
                        );
                        for id in eaten_ids {
                            if let Some((entity, _)) = game_state.spawned_food.remove(&id) {
                                commands.entity(entity).despawn();
                            }
                        }
                    }
                    ServerMessage::GameOver => {
                        info!("[CLIENT] You lost, return to the Menu...");

                        for (_, (entity, _)) in game_state.spawned_players.drain() {
                            commands.entity(entity).despawn();
                        }
                        for (_, (entity, _)) in game_state.spawned_food.drain() {
                            commands.entity(entity).despawn();
                        }
                        game_state.my_id = None;

                        net.server_connection = None;
                        net.reliable_stream = None;
                        net.unreliable_stream = None;

                        next_state.set(AppState::LoginMenu);
                    }
                }
            } else {
                error!("[CLIENT] Failed to deserialize server message");
            }
        }
        BrokerMessage::BroadcastChatMessage { username, msg } => {
            // Convert the fixed-size byte array back to a string, trimming any trailing null bytes
            let username_str = String::from_utf8_lossy(&username)
                .trim_end_matches(char::from(0))
                .to_string();

            let msg_str = String::from_utf8_lossy(&msg)
                .trim_end_matches(char::from(0))
                .to_string();

            info!(
                "[CLIENT] Chat message received from {}: {}",
                username_str, msg_str
            );
            // Add the received chat message to the chat history
            chat_state
                .chat_history
                .push(format!("{} : {}", username_str, msg_str));
        }
        _ => {
            // The client ignores other types of messages from the broker
        }
    }
}

// This function processes the AOI snapshot received from the server, which contains the list of players currently in the client's area of interest.
// It updates existing player entities, spawns new ones, and despawns those that are no longer present.
fn handle_aoi_snapshot(
    players: Vec<shared::PlayerState>,
    commands: &mut Commands,
    game_state: &mut crate::game::GameState,
    targets: &mut Query<&mut TargetTransform>,
    current_time: f64,
    meshes: &mut ResMut<Assets<Mesh>>,
    materials: &mut ResMut<Assets<ColorMaterial>>,
) {
    // Uncomment for detailed per-snapshot logging:
    // debug!("[CLIENT] Received AOI with {} players", players.len());

    let mut current_frame_ids = Vec::new();

    for p in players {
        current_frame_ids.push(p.id);

        let current_radius = 15.0 + p.score;

        if let Some(&mut (entity, ref mut last_seen)) = game_state.spawned_players.get_mut(&p.id) {
            // Existing player in AOI, update their position and last seen timestamp
            *last_seen = current_time;
            if let Ok(mut target) = targets.get_mut(entity) {
                target.x = p.x;
                target.y = p.y;
                target.scale = current_radius;
            }
        } else {
            // new player in AOI, spawn an entity for them
            spawn_player(commands, meshes, materials, game_state, &p, current_time);
        }
    }
}
