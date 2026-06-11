use crate::ApiState;
use shared::{ErrorResponse, LoginRequest, LoginResponse, SimpleServerInfo};
use tracing::{info, warn};

use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ------------------- Data structure for Error response -------------------

// ------------------- Login handler -------------------
// Login with a username and password (might add stuff more stuff later)

pub async fn login_handler(
    state: State<ApiState>,
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

    let response = LoginResponse {
        player_uuid: Uuid::new_v4().to_string(),
        server: SimpleServerInfo {
            ip: state.broker_ip.clone(),
            port: state.broker_port,
            zone: "broker".to_string(),
        },
    };

    info!(
        "Successfully authenticated {}. Redirecting to Broker at {}:{}",
        payload.username, state.broker_ip, state.broker_port
    );
    Ok(Json(response))
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use axum::{
//         Router,
//         routing::{get, post},
//     };
//     use axum_test::{TestServer, TestServerConfig, Transport};
//     use shared::{DEFAULT_REDIS_IP, LoginRequest};

//     #[tokio::test]
//     async fn test_health_check() {
//         let app = Router::new().route("/health", get(health_handler));
//         let server = TestServer::new(app);
//         let response = server.get("/health").await;
//         response.assert_status_ok();
//         response.assert_json(&serde_json::json!({ "status": "ok" }));
//     }

// #[tokio::test]
// async fn test_login_invalid_password() {
//     let redis_conn = shared::init_redis(DEFAULT_REDIS_IP).await.unwrap();
//     let state = ApiState { redis_conn };

//     let app = Router::new()
//         .route("/login", post(login_handler))
//         .with_state(state);

//     let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
//     // This server config tells a axum-test to create a REAL HTTP server listening on a RANDOM PORT, instead of using the default in-memory transport.
//     let config = TestServerConfig {
//         transport: Some(Transport::HttpRandomPort),
//         ..Default::default()
//     };

//     let server = TestServer::new_with_config(service, config);

//     let payload = LoginRequest {
//         username: "TestPlayer".to_string(),
//         password: "mauvais_mot_de_passe".to_string(),
//     };

//     let response = server.post("/login").json(&payload).await;

//     response.assert_status_service_unavailable();

//     let json_response = response.json::<serde_json::Value>();
//     assert_eq!(json_response["error"], "invalid credentials");
// }

// #[tokio::test]
// async fn test_login_success() {
//     use redis::AsyncCommands;

//     let redis_conn = shared::init_redis("redis://127.0.0.1/").await.unwrap();
//     let mut cmd_conn = redis_conn.clone();

//     let redis_key = "server:15.23.42.11:9001";
//     let _: () = cmd_conn.del(redis_key).await.unwrap();

//     let fake_server = shared::ServerInfo {
//        id: 1, // does not matter for a test
//         ip: "15.23.42.11".to_string(),
//         port: 9001,
//         zone: "eu-west".to_string(),
//         num_players: 0,
//         capacity: 100,
//         lat: 48.8566,
//         lon: 2.3522,
//         status: "available".to_string(),
//         cpu_usage: 0.0,
//         mem_usage: 0,
//     };

//     let fake_server_json = serde_json::to_string(&fake_server).unwrap();
//     let _: () = cmd_conn
//         .hset(redis_key, "data", fake_server_json)
//         .await
//         .unwrap();

//     let state = ApiState { redis_conn };
//     let app = Router::new()
//         .route("/login", post(login_handler))
//         .with_state(state);

//     let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
//     let config = TestServerConfig {
//         transport: Some(Transport::HttpRandomPort),
//         ..Default::default()
//     };
//     let server = TestServer::new_with_config(service, config);

//     let payload = LoginRequest {
//         username: "GamerPro".to_string(),
//         password: "1234".to_string(),
//     };

//     let response = server.post("/login").json(&payload).await;

//     response.assert_status_ok();

//     let json_response = response.json::<serde_json::Value>();

//     // Make sure the response contains a player_uuid and the correct server info
//     assert!(json_response["player_uuid"].is_string());
//     assert!(!json_response["player_uuid"].as_str().unwrap().is_empty());
//     assert_eq!(json_response["server"]["ip"], "15.23.42.11");
//     assert_eq!(json_response["server"]["port"], 9001);
//     assert_eq!(json_response["server"]["zone"], "eu-west");

//     let _: () = cmd_conn.del("game_servers").await.unwrap();
// }
// }
