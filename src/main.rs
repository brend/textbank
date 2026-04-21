//! TextBank gRPC server binary.
//!
//! This binary starts the TextBank gRPC service and serves requests
//! on the configured address and port.

use std::sync::Arc;
use textbank::{pb::text_bank_server::TextBankServer, Store, Svc, WalDurability};
use tokio::signal;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting TextBank server...");
    let wal_durability = WalDurability::from_env("TEXTBANK_DURABILITY")?;

    // Create the store with WAL persistence
    let store = Arc::new(Store::new("data/wal.jsonl", wal_durability)?);

    // Create the gRPC service
    let svc = Svc { store };

    // Configure server address
    let addr = "127.0.0.1:50051".parse()?;

    println!(
        "TextBank server listening on {} (wal durability: {:?})",
        addr, wal_durability
    );

    // Start the gRPC server
    Server::builder()
        .add_service(TextBankServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    Ok(())
}

/// Waits for a shutdown signal (Ctrl+C).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    println!("Shutdown signal received, stopping server...");
}
