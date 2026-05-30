use redis::{AsyncCommands, aio::MultiplexedConnection};
use shared::ServerInfo;
use std::net::UdpSocket;
use std::time::Instant;
use tokio::time::{Duration, interval};
use tracing::{error, info};

//Settings for the spawner. Adjust as needed for testing or production.
const HOT_SERVERS_MIN: usize = 3;
const MAX_PLAYERS_PER_SERVER: u16 = 3;
const STARTING_PORT: u16 = 8001;
const MAX_PORT: u16 = 9000;
const TICKING_INTERVAL_SECS: u64 = 5;
const BOOT_TIMEOUT_SECS: u64 = 20;

pub async fn maintain_hot_servers(mut redis_conn: MultiplexedConnection) {
    println!(
        "Scaler started. Minimum available servers required: {}",
        HOT_SERVERS_MIN
    );

    let mut ticker = interval(Duration::from_secs(TICKING_INTERVAL_SECS));
    let mut port_cursor: u16 = STARTING_PORT;
    let mut pending_spawns: Vec<Instant> = Vec::new();
    let boot_timeout = Duration::from_secs(BOOT_TIMEOUT_SECS);

    loop {
        //We get all the servers from Redis and count how many are available (currently just "not full" but later we can use cpu usage healthiness too).
        //If we have less than HOT_SERVERS_MIN, we spawn new servers until we reach the minimum.
        ticker.tick().await;

        // Clear out pending spawns that took too long (they probably crashed on boot)
        let now = Instant::now();
        pending_spawns.retain(|&spawn_time| now.duration_since(spawn_time) < boot_timeout);

        let available_count = count_available_servers(&mut redis_conn).await;
        let projected_count = available_count + pending_spawns.len();

        info!(
            "Cluster Status: {} available, {} booting. Target: {}.",
            available_count,
            pending_spawns.len(),
            HOT_SERVERS_MIN
        );

        if projected_count < HOT_SERVERS_MIN {
            let servers_to_spawn = HOT_SERVERS_MIN - projected_count;
            info!("Need {} more servers. Spawning...", servers_to_spawn);

            for _ in 0..servers_to_spawn {
                // Find the next genuinely free port
                let free_port = find_free_port(&mut port_cursor);

                // Spawn the server with the guaranteed free port
                spawn_dedicated_server(free_port, "Canada", MAX_PLAYERS_PER_SERVER).await;

                // Track this spawn so we don't spawn it again on the next tick
                pending_spawns.push(now);
            }
        }
    }
}

// Scans Redis for all active servers, downloads their JSON from the "data" field,
// and counts how many are marked as "available".
async fn count_available_servers(redis_conn: &mut MultiplexedConnection) -> usize {
    let mut available = 0;
    let mut server_keys = Vec::new();

    // SCAN for all server keys
    {
        let mut scan_iter = match redis_conn.scan_match::<_, String>("server:*").await {
            Ok(iter) => iter,
            Err(e) => {
                error!("Error scanning Redis for servers: {}", e);
                return 0;
            }
        };

        while let Some(key) = scan_iter.next_item().await {
            server_keys.push(key);
        }
    }

    // HGET the "data" field and parse the JSON to get availability status
    for key in server_keys {
        if let Ok(server_json) = redis_conn.hget::<_, _, String>(&key, "data").await {
            if let Ok(info) = serde_json::from_str::<ServerInfo>(&server_json) {
                if info.status == "available" {
                    available += 1;
                }
            }
        }
    }

    available
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

async fn spawn_dedicated_server(port: u16, zone: &str, max_players: u16) {
    info!("Booting Bevy server on port {} in zone {}", port, zone);

    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };

    info!("Using profile '{}' for dedicated server executable.", profile);

    let default_path = format!(
        "./target/{}/dedicated_server{}",
        profile,
        std::env::consts::EXE_SUFFIX
    );

    let executable_path = std::env::var("DEDICATED_SERVER_PATH").unwrap_or(default_path);

    match tokio::process::Command::new(&executable_path)
        .env("DS_PORT", port.to_string())
        .env("DS_ZONE", zone)
        .env("DS_MAX_PLAYERS", max_players.to_string())
        .spawn()
    {
        Ok(_) => info!("Dedicated server started successfully."),
        Err(e) => error!(
            "CRITICAL ERROR: Failed to launch server at '{}'. Error: {}",
            executable_path, e
        ),
    }
}
