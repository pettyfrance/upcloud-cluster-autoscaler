mod provider;
mod upcloud;
mod proto {
    include!(concat!(env!("OUT_DIR"), "/mod.rs"));
}

use proto::clusterautoscaler::cloudprovider::v1::externalgrpc::cloud_provider_server::CloudProviderServer;
use provider::UpcloudProvider;
use tonic::transport::Server;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cluster_id =
        std::env::var("UPCLOUD_CLUSTER_ID").map_err(|_| "UPCLOUD_CLUSTER_ID is not set")?;
    let token = std::env::var("UPCLOUD_TOKEN").map_err(|_| "UPCLOUD_TOKEN is not set")?;

    let provider = UpcloudProvider::new(cluster_id, token).await?;
    println!(
        "plan limit {}, node group max {}",
        provider.plan_limit, provider.group_max
    );

    let addr = "0.0.0.0:8086".parse()?;
    println!("externalgrpc provider listening on {addr}");

    Server::builder()
        .add_service(CloudProviderServer::new(provider))
        .serve(addr)
        .await?;

    Ok(())
}
