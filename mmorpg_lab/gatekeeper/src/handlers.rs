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

// ------------------- Tests ---------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        routing::{get, post},
    };
    use axum_test::{TestServer, TestServerConfig, Transport};
    use shared::{DEFAULT_REDIS_IP, LoginRequest};

    #[tokio::test]
    async fn test_health_check() {
        let app = Router::new().route("/health", get(health_handler));
        let server = TestServer::new(app);
        let response = server.get("/health").await;
        response.assert_status_ok();
        response.assert_json(&serde_json::json!({ "status": "ok" }));
    }

    #[tokio::test]
    async fn test_login_invalid_password() {
        let redis_conn = shared::init_redis(DEFAULT_REDIS_IP).await.unwrap();
        let state = ApiState { redis_conn };

        let app = Router::new()
            .route("/login", post(login_handler))
            .with_state(state);

        let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
        // This server config tells a axum-test to create a REAL HTTP server listening on a RANDOM PORT, instead of using the default in-memory transport.
        let config = TestServerConfig {
            transport: Some(Transport::HttpRandomPort),
            ..Default::default()
        };

        let server = TestServer::new_with_config(service, config);

        let payload = LoginRequest {
            username: "TestPlayer".to_string(),
            password: "mauvais_mot_de_passe".to_string(),
        };

        let response = server.post("/login").json(&payload).await;

        response.assert_status_service_unavailable();

        let json_response = response.json::<serde_json::Value>();
        assert_eq!(json_response["error"], "invalid credentials");
    }

    #[tokio::test]
    async fn test_login_success() {
        use redis::AsyncCommands;

        let redis_conn = shared::init_redis("redis://127.0.0.1/").await.unwrap();
        let mut cmd_conn = redis_conn.clone();

        let redis_key = "server:15.23.42.11:9001";
        let _: () = cmd_conn.del(redis_key).await.unwrap();

        let fake_server = shared::ServerInfo {
            id: 1, // does not matter for a test
            ip: "15.23.42.11".to_string(),
            port: 9001,
            zone: "eu-west".to_string(),
            num_players: 0,
            capacity: 100,
            lat: 48.8566,
            lon: 2.3522,
            status: "available".to_string(),
            cpu_usage: 0.0,
            mem_usage: 0,
        };

        let fake_server_json = serde_json::to_string(&fake_server).unwrap();
        let _: () = cmd_conn
            .hset(redis_key, "data", fake_server_json)
            .await
            .unwrap();

        let state = ApiState { redis_conn };
        let app = Router::new()
            .route("/login", post(login_handler))
            .with_state(state);

        let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
        let config = TestServerConfig {
            transport: Some(Transport::HttpRandomPort),
            ..Default::default()
        };
        let server = TestServer::new_with_config(service, config);

        let payload = LoginRequest {
            username: "GamerPro".to_string(),
            password: "1234".to_string(),
        };

        let response = server.post("/login").json(&payload).await;

        response.assert_status_ok();

        let json_response = response.json::<serde_json::Value>();

        // Make sure the response contains a player_uuid and the correct server info
        assert!(json_response["player_uuid"].is_string());
        assert!(!json_response["player_uuid"].as_str().unwrap().is_empty());
        assert_eq!(json_response["server"]["ip"], "15.23.42.11");
        assert_eq!(json_response["server"]["port"], 9001);
        assert_eq!(json_response["server"]["zone"], "eu-west");

        let _: () = cmd_conn.del("game_servers").await.unwrap();
    }
}
