mod handlers;
mod redis_pool;

use axum::{Router, routing::get, routing::post};
use handlers::{health_handler, login_handler};

use redis_pool::ApiState;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;
use shared::{DEFAULT_GATEKEEPER_ADDR_PORT, DEFAULT_REDIS_IP};

#[tokio::main]
async fn main() {

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Fatal error: could not set up logging");

    info!("Starting gatekeeper...");

    let redis_ip = std::env::var("REDIS_IP").unwrap_or_else(|_| DEFAULT_REDIS_IP.to_string());
    let listen_addr =
        std::env::var("GATEKEEPER_ADDR_PORT").unwrap_or_else(|_| DEFAULT_GATEKEEPER_ADDR_PORT.to_string());

    // Connect to Redis
    let Ok(redis_conn) = shared::init_redis(&format!("{}", redis_ip)).await else {
        error!(
            "Fatal error: could not connect to Redis. Make sure Redis is running and accessible at {}",
            redis_ip
        );
        return;
    };

    let state = ApiState { redis_conn };

    let app = Router::new()
        .route("/login", post(login_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    // Bind to TCP port
    let Ok(listener) = tokio::net::TcpListener::bind(&listen_addr).await else {
        error!(
            address = listen_addr,
            "Fatal error: could not bind to TCP port"
        );
        return;
    };

    info!(address = listen_addr, "API listening");

    if let Err(e) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    {
        error!(error = ?e, "Fatal error (server crashed ??)");
    }
}
