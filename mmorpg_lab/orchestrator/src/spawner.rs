use shared::{
    DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT, DEFAULT_ORCH_HEARTBEAT_PORT, DEFAULT_ORCHESTRATOR_ADDR,
};
use std::net::UdpSocket;
use tokio::sync::mpsc;
use tracing::{error, info};

//Settings for the spawner. Adjust as needed for testing or production.
// const HOT_SERVERS_MIN: usize = 1;
const MAX_PLAYERS_PER_SERVER: u16 = 50;
const STARTING_PORT: u16 = 8001;
const MAX_PORT: u16 = 9000;
// const TICKING_INTERVAL_SECS: u64 = 5;
// const BOOT_TIMEOUT_SECS: u64 = 20;

pub async fn maintain_hot_servers(
    mut spawn_rx: mpsc::UnboundedReceiver<(u32, u32, shared::rect::Rect)>,
) {
    info!("Lazy Spawner started. Booting default lobby (shard:0)...");

    let mut port_cursor: u16 = STARTING_PORT;

    // Starts with a default lobby (shard:0) always up and running
    let initial_port = find_free_port(&mut port_cursor);
    let lobby_bounds = shared::rect::Rect {
        x: shared::MAP_BOUND_MIN,
        y: shared::MAP_BOUND_MIN,
        width: shared::MAP_SIZE,
        height: shared::MAP_SIZE,
    };
    spawn_dedicated_server(
        initial_port,
        "shard:0",
        MAX_PLAYERS_PER_SERVER,
        0,
        0,
        lobby_bounds,
    )
    .await;
    info!("Default lobby (shard:0) launched. Awaiting split requests from Spatial Server...");

    // Spawn hot servers on demand as split requests come in from the Spatial Server,
    // and assign them a given shard ID and a free port.
    while let Some((new_shard_id, parent_shard_id, bounds)) = spawn_rx.recv().await {
        // ignores shard 0
        if new_shard_id == 0 {
            continue;
        }

        info!(
            "Split request received: launching new shard with id {}",
            new_shard_id
        );
        let free_port = find_free_port(&mut port_cursor);
        let zone_name = format!("shard:{}", new_shard_id);

        spawn_dedicated_server(
            free_port,
            &zone_name,
            MAX_PLAYERS_PER_SERVER,
            new_shard_id,
            parent_shard_id,
            bounds,
        )
        .await;
    }
}

// Helper function to scan for a free port safely.
// Go over the ports and ping them to see if they are actually free, instead of just assuming the next one is free.
fn find_free_port(cursor: &mut u16) -> u16 {
    loop {
        // Prevent it from going over the maximum allowed port limit
        if *cursor > MAX_PORT {
            *cursor = STARTING_PORT; // Wrap around and start searching from the beginning
        }

        let test_addr = format!("0.0.0.0:{}", *cursor);

        // Try to bind. If it works, the port is free!
        if UdpSocket::bind(&test_addr).is_ok() {
            let found_port = *cursor;
            *cursor += 1;
            return found_port;
        }

        // If we reach here, the bind failed (port is used). Increment and try the next one.
        *cursor += 1;
    }
}

async fn spawn_dedicated_server(
    port: u16,
    zone: &str,
    max_players: u16,
    shard_id: u32,
    parent_shard_id: u32,
    bounds: shared::rect::Rect,
) {
    info!("Booting Bevy server on port {} in zone {}", port, zone);

    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    info!(
        "Using profile '{}' for dedicated server executable.",
        profile
    );

    let default_path = format!(
        "./target/{}/dedicated_server{}",
        profile,
        std::env::consts::EXE_SUFFIX
    );

    let orch_addr = std::env::var("ORCH_ADDR").unwrap_or_else(|_| {
        format!(
            "{}:{}",
            DEFAULT_ORCHESTRATOR_ADDR, DEFAULT_ORCH_HEARTBEAT_PORT
        )
    });
    let broker_addr = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| format!("{}:{}", DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT));

    let executable_path = std::env::var("DEDICATED_SERVER_PATH").unwrap_or(default_path);

    match tokio::process::Command::new(&executable_path)
        .env("DS_PORT", port.to_string())
        .env("DS_ZONE", zone)
        .env("DS_MAX_PLAYERS", max_players.to_string())
        .env("DS_SHARD_ID", shard_id.to_string())
        .env("DS_PARENT_SHARD_ID", parent_shard_id.to_string())
        .env("DS_BOUND_X", bounds.x.to_string())
        .env("DS_BOUND_Y", bounds.y.to_string())
        .env("DS_BOUND_W", bounds.width.to_string())
        .env("DS_BOUND_H", bounds.height.to_string())
        .env("ORCH_ADDR", orch_addr)
        .env("BROKER_ADDR", broker_addr)
        .spawn()
    {
        Ok(_) => info!("Dedicated server started successfully."),
        Err(e) => error!(
            "CRITICAL ERROR: Failed to launch server at '{}'. Error: {}",
            executable_path, e
        ),
    }
}
