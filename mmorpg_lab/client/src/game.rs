use bevy::prelude::*;
use bytes::Bytes;
use shared::ClientMessage;
use shared::broker_protocol::BrokerMessage;
use std::collections::HashMap;

use crate::network::ClientNetworkManager;
use crate::state::AppState;

pub struct GamePlugin;

#[derive(Resource, Default)]
pub struct GameState {
    pub my_id: Option<u32>,
    pub spawned_players: HashMap<u32, (Entity, f64)>, // maps client ID to the corresponding player entity in the world with its last update timestamp (for interpolation)
}

#[derive(Component)] // Component to store the server's target position for interpolation
pub struct TargetPosition {
    pub x: f32,
    pub y: f32,
}

#[derive(Component)]
pub struct PlayerComponent; // Tag component to identify player entities

impl Plugin for GamePlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<GameState>()
            .add_systems(OnEnter(AppState::InGame), setup_map)
            .add_systems(
                Update,
                (player_input, smooth_movement, move_camera).run_if(in_state(AppState::InGame)),
            );
    }
}

fn setup_map(mut commands: Commands) {
    // Floor
    commands.spawn((
        Sprite {
            color: Color::srgb(0.2, 0.5, 0.2),
            custom_size: Some(Vec2::new(800.0, 800.0)),
            ..default()
        },
        Transform::from_xyz(0.0, 0.0, -1.0),
    ));
}

fn player_input(
    keyboard: Res<ButtonInput<KeyCode>>,
    mut net: ResMut<ClientNetworkManager>,
    game_state: Res<GameState>,
) {
    let mut x = 0.0;
    let mut y = 0.0;

    if keyboard.pressed(KeyCode::KeyW) || keyboard.pressed(KeyCode::ArrowUp) {
        y += 1.0;
    }
    if keyboard.pressed(KeyCode::KeyS) || keyboard.pressed(KeyCode::ArrowDown) {
        y -= 1.0;
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

fn smooth_movement(time: Res<Time>, mut query: Query<(&mut Transform, &TargetPosition)>) {
    // the higher the lerp_factor, the snappier the movement (less interpolation).
    // The lower, the smoother but more delayed. We can adjust it based on the network conditions or player preferences.
    let lerp_factor = 15.0 * time.delta_secs();

    for (mut transform, target) in &mut query {
        let target_vec = Vec3::new(target.x, target.y, transform.translation.z);

        // Interpolate the current position towards the target position using linear interpolation (lerp)
        transform.translation = transform.translation.lerp(target_vec, lerp_factor);
    }
}
