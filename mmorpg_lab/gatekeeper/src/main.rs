mod handlers;
mod redis_pool;

use axum::{Router, routing::get, routing::post};
use handlers::{health_handler, login_handler};
use redis_pool::ApiState;


#[tokio::main]
async fn main() {
    println!("Starting gatekeeper...");

    // Connect to Redis
    let Ok(redis_conn) = redis_pool::init_redis("redis://127.0.0.1/").await else {
        eprintln!("Fatal error : could not connect to Redis");
        eprintln!("Make sure Redis is running and accessible at redis://127.0.0.1/");
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
        eprintln!("Fatal error : could not bind to {}", listen_addr);
        return;
    };

    println!("API listening on http://{}", listen_addr);

    if let Err(e) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    {
        eprintln!("Fatal error (server crashed ??) : {}", e);
    }
}
