#[cfg(feature = "quic")]
mod quic_protocol;
#[cfg(feature = "quic")]
pub use quic_protocol::QuicBackend;

#[cfg(feature = "gns")]
mod gns_protocol;
#[cfg(feature = "gns")]
pub use gns_protocol::GnsBackend;
