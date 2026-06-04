pub mod quadtree;
pub mod rect;
pub mod spatialservice;

use crate::spatialservice::SpatialService;

use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("Fatal Error: unable to set up logging subscriber");

    info!("Starting MMORPG spatial server...");

    let addr = "127.0.0.1".into();
    let mut spatial_service = SpatialService::new(&addr, &10000, &addr, &10002);

    spatial_service.run();
}
