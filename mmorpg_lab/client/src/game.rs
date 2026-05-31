use bevy::prelude::*;
use bytes::Bytes;
use shared::ClientMessage;
use std::collections::HashMap;
use uuid::Uuid;

use crate::network::ClientNetworkManager;
use crate::state::AppState;

use tracing::warn;

pub struct GamePlugin;

#[derive(Resource, Default)]
pub struct GameState {
    pub my_id: Option<Uuid>,
    pub spawned_players: HashMap<Uuid, Entity>, // maps client ID to the corresponding player entity in the world
}

#[derive(Component)]
pub struct PlayerComponent; // Tag component to identify player entities

impl Plugin for GamePlugin {
    fn build(&self, app: &mut App) {
        app.init_resource::<GameState>()
            .add_systems(OnEnter(AppState::InGame), setup_map)
            .add_systems(
                Update,
                (player_input, move_camera).run_if(in_state(AppState::InGame)),
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

fn player_input(keyboard: Res<ButtonInput<KeyCode>>, mut net: ResMut<ClientNetworkManager>) {
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
        if let (Some(peer), Some(conn), Some(stream)) = (&mut net.peer, conn_opt, stream_opt) {
            let msg = ClientMessage::MoveInput { x, y };
            if let Ok(bytes) = bincode::serialize(&msg) {
                if let Err(e) = peer.send(&conn, &stream, Bytes::from(bytes)) {
                    tracing::warn!("Local input loss : {:?}", e);
                }
            }
        } else {
            warn!("[CLIENT] : Cannot send input, not fully connected yet.");
        }
    }
}

fn move_camera(
    game_state: Res<GameState>,
    player_query: Query<&Transform, With<PlayerComponent>>,
    mut camera_query: Query<&mut Transform, (With<Camera>, Without<PlayerComponent>)>,
) {
    if let Some(my_id) = game_state.my_id {
        if let Some(&entity) = game_state.spawned_players.get(&my_id) {
            if let Ok(player_transform) = player_query.get(entity) {
                if let Ok(mut camera_transform) = camera_query.single_mut() {
                    camera_transform.translation.x = player_transform.translation.x;
                    camera_transform.translation.y = player_transform.translation.y;
                }
            }
        }
    }
}
