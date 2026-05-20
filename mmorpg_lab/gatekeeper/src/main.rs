mod handlers;
mod redis_pool;

use axum::{Router, routing::get, routing::post};
use handlers::{health_handler, login_handler};
use redis_pool::ApiState;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Erreur fatale : impossible d'initialiser tracing");

    info!("Starting gatekeeper...");

    // Connect to Redis
    let Ok(redis_conn) = shared::init_redis("redis://127.0.0.1/").await else {
        error!(
            "Fatal error: could not connect to Redis. Make sure Redis is running and accessible at redis://127.0.0.1/"
        );
        return;
    };

    let state = ApiState { redis_conn };

    let app = Router::new()
        .route("/login", post(login_handler))
        .route("/health", get(health_handler))
        .with_state(state);

    let listen_addr = "127.0.0.1:8080";

    // Bind to TCP port
    let Ok(listener) = tokio::net::TcpListener::bind(listen_addr).await else {
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
