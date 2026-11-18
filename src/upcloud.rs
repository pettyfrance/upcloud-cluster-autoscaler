use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::BoxError;

const API_BASE: &str = "https://api.upcloud.com/1.3";

#[derive(Debug, Deserialize)]
pub(crate) struct UksCluster {
    pub(crate) plan: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct UksNodeGroup {
    pub(crate) name: String,
    pub(crate) count: i32,
    /// Group state, e.g. "running", "scaling-up", "failed".
    #[serde(default)]
    pub(crate) state: String,
    /// Marked experimental in the UpCloud API docs. Defaults to empty if a
    /// response leaves it out.
    #[serde(default)]
    pub(crate) nodes: Vec<UksNode>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct UksNode {
    /// Kubernetes node name.
    pub(crate) name: String,
    /// UpCloud server UUID; the node's providerID is `upcloud:////<uuid>`.
    pub(crate) uuid: String,
    /// Node state, e.g. "running".
    pub(crate) state: String,
}

#[derive(Debug, Serialize)]
struct NodeGroupCountUpdate {
    count: i32,
}

pub(crate) struct UpcloudApi {
    http: reqwest::Client,
    cluster_id: String,
    token: String,
}

impl UpcloudApi {
    pub(crate) fn new(cluster_id: String, token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            cluster_id,
            token,
        }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, BoxError> {
        let url = format!("{API_BASE}/kubernetes/{}{path}", self.cluster_id);
        let resp = self
            .http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json().await?)
    }

    pub(crate) async fn fetch_cluster(&self) -> Result<UksCluster, BoxError> {
        self.get("").await
    }

    pub(crate) async fn fetch_node_groups(&self) -> Result<Vec<UksNodeGroup>, BoxError> {
        self.get("/node-groups").await
    }

    /// Fetches one node group, including its nodes.
    pub(crate) async fn fetch_node_group(&self, name: &str) -> Result<UksNodeGroup, BoxError> {
        self.get(&format!("/node-groups/{name}")).await
    }

    /// Deletes one specific node from a node group.
    pub(crate) async fn delete_node(&self, group: &str, node: &str) -> Result<(), BoxError> {
        let url = format!(
            "{API_BASE}/kubernetes/{}/node-groups/{group}/{node}",
            self.cluster_id
        );
        self.http
            .delete(url)
            .bearer_auth(&self.token)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Sets a node group's desired node count. UKS adds or removes nodes to match.
    pub(crate) async fn set_node_group_count(
        &self,
        name: &str,
        count: i32,
    ) -> Result<(), BoxError> {
        let url = format!(
            "{API_BASE}/kubernetes/{}/node-groups/{name}",
            self.cluster_id
        );
        self.http
            .patch(url)
            .bearer_auth(&self.token)
            .json(&NodeGroupCountUpdate { count })
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// Fetches every node group with its nodes: one list call, then one
    /// details call per group. Fails if any call fails, so callers never get
    /// a partial set.
    pub(crate) async fn fetch_node_groups_with_nodes(&self) -> Result<Vec<UksNodeGroup>, BoxError> {
        let mut groups = Vec::new();
        for group in self.fetch_node_groups().await? {
            groups.push(self.fetch_node_group(&group.name).await?);
        }
        Ok(groups)
    }
}
