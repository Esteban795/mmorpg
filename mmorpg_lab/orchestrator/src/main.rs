mod listener;
mod spawner;
use shared::{DEFAULT_REDIS_IP};

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Fatal Error: unable to set up logging subscriber");

    info!("Starting MMORPG Orchestrator...");

    // Initialize Redis
    let redis_url = std::env::var("REDIS_IP").unwrap_or_else(|_| DEFAULT_REDIS_IP.to_string());
    let redis_conn = match shared::init_redis(&redis_url).await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to start orchestrator due to Redis error: {}", e);
            return;
        }
    };

    // Start the UDP heartbeat listener in a background task
    let listener_redis = redis_conn.clone();
    tokio::spawn(async move {
        listener::heartbeat_listener(listener_redis).await;
    });

    //Start the server scaling manager
    let spawner_redis = redis_conn.clone();
    spawner::maintain_hot_servers(spawner_redis).await;
}
