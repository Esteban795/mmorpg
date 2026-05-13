use axum::{Router,routing::get, routing::post};
mod handlers;

use handlers::{health_handler, login_handler};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/login", post(login_handler))
        .route("/health", get(health_handler));
    let listen_addr = "127.0.0.1:8080";

    // Bind to TCP port
    let Ok(listener) = tokio::net::TcpListener::bind(listen_addr).await else {
        eprintln!("Fatal error : could not bind to {}", listen_addr);
        return;
    };

    println!("API listening on http://{}", listen_addr);

    if let Err(e) = axum::serve(listener, app).await {
        eprintln!("Fatal error (server crashed ??) : {}", e);
    }
}
