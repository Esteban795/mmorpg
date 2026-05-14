use crate::ApiState;
use crate::redis_pool::get_servers;
use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use shared::ServerInfo;
use uuid::Uuid;

// ------------------- Data structure for Error response -------------------
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorResponse {
    pub error: String,
}



// ------------------- Login handler -------------------
// Login with a username and password (might add stuff more stuff later)
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

// Response with the IP of the game server to connect to
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginResponse {
    pub player_uuid: String,
    pub server: ServerInfo,
}

pub async fn login_handler(
    state: State<ApiState>,
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    println!(
        "New connection request from player : {} with password : {}",
        payload.username, payload.password
    );

    // Auth : accept any username with the password 1234, username does not matter
    if payload.password != "1234" {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "invalid credentials".to_string(),
            }),
        ));
    }

    // Get all game servers from redis
    let game_servers = match get_servers(&state).await {
        Ok(servers) => servers,
        Err(_) => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "unable to retrieve game servers".to_string(),
                }),
            ));
        }
    };

    // For now, always return the first server in the list (if any)
    match game_servers.len() {
        0 => {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "no game server available".to_string(),
                }),
            ));
        }
        1 => {
            let response = LoginResponse {
                player_uuid: Uuid::new_v4().to_string(),
                server: ServerInfo {
                    ip: game_servers[0].ip.clone(),
                    port: game_servers[0].port,
                    zone: game_servers[0].zone.clone(),
                },
            };

            return Ok(Json(response));
        }
        _ => {
            let response = LoginResponse {
                player_uuid: Uuid::new_v4().to_string(),
                server: ServerInfo {
                    ip: game_servers[0].ip.clone(),
                    port: game_servers[0].port,
                    zone: game_servers[0].zone.clone(),
                },
            };

            return Ok(Json(response));
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HealthResponse {
    pub status: String,
}



pub async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}
