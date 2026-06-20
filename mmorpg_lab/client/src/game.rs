use bevy::prelude::*;
use bytes::Bytes;
use shared::ClientMessage;
use shared::broker_protocol::BrokerMessage;
use std::collections::HashMap;

use crate::chatbox::{ChatPlugin, ChatState};
use crate::network::ClientNetworkManager;
use crate::state::AppState;
use shared::{BASE_PLAYER_RADIUS, MAP_SIZE};

#[derive(Resource, Default)]
pub struct GameState {
    pub my_id: Option<u32>,
    pub spawned_players: HashMap<u32, (Entity, f64)>, // maps client ID to the corresponding player entity in the world with its last update timestamp (for interpolation)
    pub spawned_food: HashMap<u32, (Entity, f64)>, // same for food, maps food ID to entity and last update timestamp
}

#[derive(Component)] // Component to store the server's target position for interpolation
pub struct TargetTransform {
    pub x: f32,
    pub y: f32,
    pub scale: f32,
}

#[derive(Component)]
pub struct PlayerComponent; // Tag component to identify player entities

#[derive(Component)]
pub struct PlayerNameText; // Tag component to identify the Text2dBundle that displays the player's name above their head

pub struct GamePlugin;
impl Plugin for GamePlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<GameState>()
            .insert_resource(ClientSettings {
                map_texture_path: "map_bg.png".to_string(),
            })
            .add_plugins(ChatPlugin)
            .add_systems(OnEnter(AppState::InGame), setup_map)
            .add_systems(
                Update,
                (player_input, smooth_transform, move_camera).run_if(in_state(AppState::InGame)),
            );
    }
}

#[derive(Resource)]
pub struct ClientSettings {
    pub map_texture_path: String,
}

fn setup_map(
    mut commands: Commands,
    asset_server: Res<AssetServer>,
    settings: Res<ClientSettings>,
) {
    // Floor
    commands.spawn((
        Sprite {
            image: asset_server.load(&settings.map_texture_path),
            //color: Color::srgb(0.2, 0.5, 0.2),
            custom_size: Some(Vec2::new(MAP_SIZE, MAP_SIZE)),
            ..default()
        },
        Transform::from_xyz(0.0, 0.0, -1.0),
    ));
}

fn player_input(
    keyboard: Res<ButtonInput<KeyCode>>,
    mut net: ResMut<ClientNetworkManager>,
    game_state: Res<GameState>,
    chat_state: Res<ChatState>,
) {
    if chat_state.is_focused {
        return;
    }

    let mut x = 0.0;
    let mut y = 0.0;

    if keyboard.pressed(KeyCode::KeyW) || keyboard.pressed(KeyCode::ArrowUp) {
        y -= 1.0;
    }
    if keyboard.pressed(KeyCode::KeyS) || keyboard.pressed(KeyCode::ArrowDown) {
        y += 1.0;
    }
    if keyboard.pressed(KeyCode::KeyA) || keyboard.pressed(KeyCode::ArrowLeft) {
        x -= 1.0;
    }
    if keyboard.pressed(KeyCode::KeyD) || keyboard.pressed(KeyCode::ArrowRight) {
        x += 1.0;
    }

    if x != 0.0 || y != 0.0 {
        let conn_opt = net.server_connection.clone();
        let stream_opt = net.unreliable_stream.clone();

        let my_id = game_state.my_id.unwrap_or(0);

        if let (Some(peer), Some(conn), Some(stream)) = (&mut net.peer, conn_opt, stream_opt) {
            let msg = ClientMessage::MoveInput { x, y };
            if let Ok(bytes) = bincode::serialize(&msg) {
                let mut input_array = [0u8; 16];
                let len = bytes.len().min(16);
                input_array[..len].copy_from_slice(&bytes[..len]);

                let broker_msg = BrokerMessage::ClientInput {
                    client_id: my_id,
                    input: input_array,
                };

                if let Err(e) = peer.send(&conn, &stream, Bytes::from(broker_msg.to_bytes())) {
                    tracing::warn!("Local input loss : {:?}", e);
                }
            }
        }
    }
}

fn move_camera(
    game_state: Res<GameState>,
    player_query: Query<&Transform, With<PlayerComponent>>,
    mut camera_query: Query<&mut Transform, (With<Camera>, Without<PlayerComponent>)>,
) {
    if let Some(my_id) = game_state.my_id {
        if let Some(&(entity, _)) = game_state.spawned_players.get(&my_id) {
            if let Ok(player_transform) = player_query.get(entity) {
                if let Ok(mut camera_transform) = camera_query.single_mut() {
                    camera_transform.translation.x = player_transform.translation.x;
                    camera_transform.translation.y = player_transform.translation.y;
                }
            }
        }
    }
}

fn smooth_transform(
    time: Res<Time>,
    mut query: Query<(&mut Transform, &TargetTransform, &Children)>,
    mut text_query: Query<&mut Transform, (With<PlayerNameText>, Without<TargetTransform>)>,
) {
    // the higher the lerp_factor, the snappier the movement (less interpolation).
    // The lower, the smoother but more delayed. We can adjust it based on the network conditions or player preferences.
    let lerp_factor = 10.0 * time.delta_secs();

    for (mut transform, target, children) in &mut query {
        let target_y = target.y;
        let target_vec = Vec3::new(target.x, -target_y, transform.translation.z);
        transform.translation = transform.translation.lerp(target_vec, lerp_factor);

        let current_scale = transform.scale.x;
        let new_scale = current_scale + (target.scale - current_scale) * lerp_factor;
        transform.scale = Vec3::new(new_scale, new_scale, 1.0);

        for child in children.iter() {
            if let Ok(mut text_transform) = text_query.get_mut(child) {
                text_transform.scale = Vec3::new(1.0 / new_scale, 1.0 / new_scale, 1.0);

                text_transform.translation.y = 1.0 + (20.0 / new_scale);
            }
        }
    }
}

pub fn spawn_food(
    commands: &mut Commands,
    meshes: &mut ResMut<Assets<Mesh>>,
    materials: &mut ResMut<Assets<ColorMaterial>>,
    game_state: &mut GameState,
    food_data: &shared::FoodData,
    current_time: f64,
) {
    if !game_state.spawned_food.contains_key(&food_data.id) {
        let entity = commands
            .spawn((
                Mesh2d(meshes.add(Circle::new(5.0))),
                MeshMaterial2d(materials.add(Color::srgb(1.0, 1.0, 0.0))),
                Transform::from_xyz(food_data.x, -food_data.y, -0.5),
            ))
            .id();
        game_state
            .spawned_food
            .insert(food_data.id, (entity, current_time));
    }
}

pub fn spawn_player(
    commands: &mut Commands,
    meshes: &mut ResMut<Assets<Mesh>>,
    materials: &mut ResMut<Assets<ColorMaterial>>,
    game_state: &mut GameState,
    player_data: &shared::PlayerState,
    current_time: f64,
) {
    let is_me = game_state.my_id == Some(player_data.id);
    let color = if is_me {
        Color::srgb(0.2, 0.2, 1.0)
    } else {
        Color::srgb(1.0, 0.2, 0.2)
    };

    // Display name truncated if too long
    let display_name = if player_data.username.len() > 10 {
        format!("{}...", &player_data.username[..8])
    } else {
        player_data.username.clone()
    };

    let base_radius = BASE_PLAYER_RADIUS; // Default radius for a player with score 0
    let current_radius = base_radius + player_data.score;

    let entity =
        commands
            .spawn((
                Mesh2d(meshes.add(Circle::new(1.0))),
                MeshMaterial2d(materials.add(color)),
                Transform::from_xyz(player_data.x, -player_data.y, 0.0).with_scale(Vec3::new(
                    current_radius,
                    current_radius,
                    1.0,
                )),
                crate::game::PlayerComponent,
                crate::game::TargetTransform {
                    x: player_data.x,
                    y: player_data.y,
                    scale: current_radius,
                },
            ))
            .with_children(|parent| {
                // Display the player's username above their character
                parent.spawn((
                    Text2d::new(display_name),
                    TextFont {
                        font_size: 15.0,
                        ..default()
                    },
                    TextColor(Color::WHITE),
                    Transform::from_xyz(0.0, 1.0 + (20.0 / current_radius), 1.0)
                        .with_scale(Vec3::new(1.0 / current_radius, 1.0 / current_radius, 1.0)),
                    crate::game::PlayerNameText,
                ));
            })
            .id();

    game_state
        .spawned_players
        .insert(player_data.id, (entity, current_time));
}
