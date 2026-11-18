mod provider;
mod upcloud;
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}

use proto::clusterautoscaler::cloudprovider::v1::externalgrpc::cloud_provider_server::CloudProviderServer;
use provider::UpcloudProvider;
use tokio::signal::unix::{SignalKind, signal};
use tonic::transport::Server;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    // Installed first, so a SIGTERM during startup isn't lost. As PID 1 in a
    // container, the process would otherwise ignore SIGTERM entirely.
    let mut sigterm = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = sigterm.recv() => println!("received SIGTERM, shutting down"),
            _ = tokio::signal::ctrl_c() => println!("received Ctrl+C, shutting down"),
        }
    };

    let cluster_id =
        std::env::var("UPCLOUD_CLUSTER_ID").map_err(|_| "UPCLOUD_CLUSTER_ID is not set")?;
    let token = std::env::var("UPCLOUD_TOKEN").map_err(|_| "UPCLOUD_TOKEN is not set")?;

    let provider = UpcloudProvider::new(cluster_id, token).await?;
    println!(
        "plan limit {}, node group max {}",
        provider.plan_limit, provider.group_max
    );

    // Localhost only: the provider runs as a sidecar next to the
    // cluster-autoscaler, and nothing outside the pod may call it.
    let addr = "127.0.0.1:8086".parse()?;
    println!("externalgrpc provider listening on {addr}");

    Server::builder()
        .add_service(CloudProviderServer::new(provider))
        // Stops accepting connections on shutdown and lets in-flight requests
        // (e.g. a resize) finish.
        .serve_with_shutdown(addr, shutdown)
        .await?;

    println!("provider stopped");
    Ok(())
}
