use std::net::SocketAddr;

use crate::ApiState;
use crate::redis_pool::get_servers;
use shared::{ErrorResponse, LoginRequest, LoginResponse, SimpleServerInfo};
use tracing::{error, info, warn};

use axum::{
    Json,
    extract::{ConnectInfo, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ------------------- Geoloc api handler -------------------

#[derive(Deserialize)]
struct GeoResponse {
    lat: f64,
    lon: f64,
}

fn calculate_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6371.0_f64;
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1 = lat1.to_radians();
    let lat2 = lat2.to_radians();

    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    r * c
}

// ------------------- Data structure for Error response -------------------

// ------------------- Login handler -------------------
// Login with a username and password (might add stuff more stuff later)

pub async fn login_handler(
    state: State<ApiState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!(
        "New connection request from player : {} with password : {}",
        payload.username, payload.password
    );

    // Auth : accept any username with the password 1234, username does not matter
    if payload.password != "1234" {
        warn!("Player {} provided invalid credentials", payload.username);
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "invalid credentials".to_string(),
            }),
        ));
    }

    // Get all game servers that have at least room for one player from redis
    let game_servers = match get_servers(&state).await {
        Ok(servers) => servers,
        Err(_) => {
            error!(
                "No game servers available when player {} tried to log in",
                payload.username
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "unable to retrieve game servers".to_string(),
                }),
            ));
        }
    };

    match game_servers.len() {
        0 => {
            error!(
                "No game servers available when player {} tried to log in",
                payload.username
            );
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "no game server available".to_string(),
                }),
            ));
        }
        1 => {
            warn!(
                "Only one game server available. Player {} will be connected to it without geolocation",
                payload.username
            );
            let response = LoginResponse {
                player_uuid: Uuid::new_v4().to_string(),
                server: SimpleServerInfo {
                    ip: game_servers[0].ip.clone(),
                    port: game_servers[0].port,
                    zone: game_servers[0].zone.clone(),
                },
            };

            return Ok(Json(response));
        }
        _ => {
            info!(
                "Multiple game servers available. Attempting to geolocate player {} and connect to the closest server",
                payload.username
            );
            // If more than one server is available, try to geolocate the player and return the closest server found
            let user_ip = addr.ip().to_string();
            let geo_url = format!("http://ip-api.com/json/{}", user_ip);

            if let Ok(response) = reqwest::get(&geo_url).await {
                if let Ok(geo_data) = response.json::<GeoResponse>().await {
                    let closest_server_option = game_servers
                        .iter()
                        .map(|server| {
                            let dist = calculate_distance(
                                geo_data.lat,
                                geo_data.lon,
                                server.lat,
                                server.lon,
                            );
                            (dist, server)
                        })
                        .min_by(|(dist_a, _), (dist_b, _)| {
                            dist_a
                                .partial_cmp(dist_b)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });

                    if let Some((_, best_server)) = closest_server_option {
                        info!(
                            "Located player {}. Closest server: {} (Zone: {})",
                            payload.username, best_server.ip, best_server.zone
                        );

                        return Ok(Json(LoginResponse {
                            player_uuid: Uuid::new_v4().to_string(),
                            server: SimpleServerInfo {
                                ip: best_server.ip.clone(),
                                port: best_server.port,
                                zone: best_server.zone.clone(),
                            },
                        }));
                    }
                }
            }

            info!(
                "Could not geolocate player {} or determine closest server. Falling back to first available server.",
                payload.username
            );
            let fallback_server = &game_servers[0];

            Ok(Json(LoginResponse {
                player_uuid: Uuid::new_v4().to_string(),
                server: SimpleServerInfo {
                    ip: fallback_server.ip.clone(),
                    port: fallback_server.port,
                    zone: fallback_server.zone.clone(),
                },
            }))
        }
    }
}

// ------------------- Health check handler -------------------
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HealthResponse {
    pub status: String,
}

pub async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}
