pub mod quadtree;
pub mod rect;
pub mod spatialservice;

use crate::spatialservice::SpatialService;

use shared::{DEFAULT_BROKER_IP, DEFAULT_BROKER_PORT, DEFAULT_ORCHESTRATOR_ADDR, DEFAULT_ORCHESTRATOR_PORT};
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Fatal Error: unable to set up logging subscriber");

    info!("Starting MMORPG spatial server...");

    let broker_addr: String = std::env::var("BROKER_ADDR")
        .unwrap_or_else(|_| DEFAULT_BROKER_IP.to_string())
        .parse()
        .expect("Invalid BROKER_ADDR");

    let broker_port: u16 = std::env::var("BROKER_PORT")
        .unwrap_or_else(|_| DEFAULT_BROKER_PORT.to_string())
        .parse()
        .expect("Invalid BROKER_PORT");

    let orchestrator_addr: String = std::env::var("ORCHESTRATOR_ADDR")
        .unwrap_or_else(|_| DEFAULT_ORCHESTRATOR_ADDR.to_string())
        .parse()
        .expect("Invalid ORCHESTRATOR_ADDR");
    
    let orchestrator_port: u16 = std::env::var("ORCHESTRATOR_PORT")
        .unwrap_or_else(|_| DEFAULT_ORCHESTRATOR_PORT.to_string())
        .parse()
        .expect("Invalid ORCHESTRATOR_PORT");

    let mut spatial_service = SpatialService::new(&broker_addr, &broker_port, &orchestrator_addr, &orchestrator_port);

    spatial_service.run();
}
