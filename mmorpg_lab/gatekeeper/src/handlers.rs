use serde::{Deserialize, Serialize};
use shared::ServerInfo;
use axum::{Json, http::StatusCode};
use uuid::Uuid;

// Register with a username (might add stuff more stuff later)
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HealthResponse {
    pub status: String,
}

pub async fn login_handler(
    Json(payload): Json<LoginRequest>,
) -> Result<Json<LoginResponse>, (StatusCode, Json<ErrorResponse>)> {
    println!(
        "New connection request from player : {} with password : {}",
        payload.username, payload.password
    );

    // Auth : accept any username with the password 1234
    if payload.password != "1234" {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "invalid credentials".to_string(),
            }),
        ));
    }

    // TODO : find a free game server for this player, and return its IP address

    let response = LoginResponse {
        player_uuid: Uuid::new_v4().to_string(),
        server: ServerInfo {
            ip: "127.0.0.1:9000".to_string(),
            port: 9000,
            zone: "starter_zone".to_string(),
        },
    };

    Ok(Json(response))
}

pub async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}