use bevy::prelude::*;
use shared::ServerInfo;
use std::net::UdpSocket;
use tracing::{error, info, warn};

use crate::ServerConfig;
use crate::network::PlayerRegistry;

#[derive(Resource)]
pub struct HeartbeatSocket(pub UdpSocket);

#[derive(Resource)]
pub struct HeartbeatTimer(pub Timer);

pub struct HeartbeatPlugin;

impl Plugin for HeartbeatPlugin {
    fn build(&self, app: &mut App) {
        app.insert_resource(HeartbeatTimer(Timer::from_seconds(
            5.0,
            TimerMode::Repeating,
        )))
        .add_systems(Update, send_heartbeat);
    }
}

fn send_heartbeat(
    time: Res<Time>,
    mut timer: ResMut<HeartbeatTimer>,
    config: Res<ServerConfig>,
    registry: Res<PlayerRegistry>,
    socket: Res<HeartbeatSocket>,
) {
    // Execute every 5 seconds while being called at 20Hz
    if timer.0.tick(time.delta()).just_finished() {
        info!("Sending heartbeat...");
        let current_players = registry.players.len() as u16;

        // Dynamically determine server status based on player count
        let status = if current_players >= config.max_players {
            "full".to_string()
        } else {
            "available".to_string()
        };

        let hb = ServerInfo {
            ip: config.ip.clone(),
            port: config.port,
            zone: config.zone.clone(),
            num_players: registry.players.len() as u16,
            capacity: config.max_players,
            status,
            lat: 0.0,
            lon: 0.0,
            cpu_usage: 0.0,
            mem_usage: 0,
        };
        info!(
            "Heartbeat info: IP={}, Port={}, Zone={}, Players={}/{}",
            hb.ip, hb.port, hb.zone, hb.num_players, hb.capacity
        );

        match serde_json::to_string(&hb) {
            Ok(payload) => {
                if let Err(e) = socket
                    .0
                    .send_to(payload.as_bytes(), config.orchestrator_addr)
                {
                    error!("Failed to send heartbeat: {}", e);
                } else {
                    info!(
                        "Heartbeat sent (Players: {}/{})",
                        hb.num_players, hb.capacity
                    );
                }
            }
            Err(e) => warn!("Failed to serialize heartbeat: {}", e),
        }
    }
}
