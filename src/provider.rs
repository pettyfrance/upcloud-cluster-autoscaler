use std::sync::RwLock;

use tonic::{Request, Response, Status};

use crate::BoxError;
use crate::proto::clusterautoscaler::cloudprovider::v1::externalgrpc::{
    CleanupRequest, CleanupResponse, Instance, InstanceErrorInfo, InstanceStatus, NodeGroup,
    NodeGroupDecreaseTargetSizeRequest, NodeGroupDecreaseTargetSizeResponse,
    NodeGroupDeleteNodesRequest, NodeGroupDeleteNodesResponse, NodeGroupForNodeRequest,
    NodeGroupForNodeResponse, NodeGroupIncreaseSizeRequest, NodeGroupIncreaseSizeResponse,
    NodeGroupNodesRequest, NodeGroupNodesResponse, NodeGroupTargetSizeRequest,
    NodeGroupTargetSizeResponse, NodeGroupsRequest, NodeGroupsResponse, RefreshRequest,
    RefreshResponse, cloud_provider_server::CloudProvider, instance_status::InstanceState,
};
use crate::upcloud::{UksNode, UksNodeGroup, UpcloudApi};

/// Minimum size of every node group. UKS doesn't allow empty node groups.
const MIN_NODES_PER_GROUP: i32 = 1;

/// Maximum `count` UKS accepts for a single node group.
/// Taken from UpCloud API Docs
const MAX_NODES_PER_GROUP: i32 = 16;

pub(crate) struct UpcloudProvider {
    api: UpcloudApi,
    pub(crate) plan_limit: i32,
    /// Max size of every node group: the plan limit, capped at the UKS per-group max.
    pub(crate) group_max: i32,
    node_groups: RwLock<Vec<UksNodeGroup>>,
}

impl UpcloudProvider {
    pub(crate) async fn new(cluster_id: String, token: String) -> Result<Self, BoxError> {
        let api = UpcloudApi::new(cluster_id, token);

        let cluster = api.fetch_cluster().await?;
        let plan_limit = plan_limit(&cluster.plan)?;
        let groups = api.fetch_node_groups_with_nodes().await?;

        Ok(Self {
            api,
            plan_limit,
            group_max: plan_limit.min(MAX_NODES_PER_GROUP),
            node_groups: RwLock::new(groups),
        })
    }

    /// Builds the autoscaler's view of a group. Every UKS group is managed,
    /// with the same limits.
    fn to_node_group(&self, group: &UksNodeGroup) -> NodeGroup {
        NodeGroup {
            id: group.name.clone(),
            min_size: MIN_NODES_PER_GROUP,
            max_size: self.group_max,
            debug: format!("{} (current size {})", group.name, group.count),
        }
    }
}

fn plan_limit(plan: &str) -> Result<i32, BoxError> {
    match plan {
        p if p.starts_with("dev") => Ok(30),
        p if p.starts_with("prod") => Ok(120),
        other => Err(format!("unknown UKS cluster plan '{other}'").into()),
    }
}

/// Prefix of a UKS node's Kubernetes `spec.providerID`; the server UUID follows.
const PROVIDER_ID_PREFIX: &str = "upcloud:////";

/// The autoscaler's catch-all error class (`OtherErrorClass`).
const OTHER_ERROR_CLASS: i32 = 99;

/// Translates a UKS node state into the autoscaler's instance status.
/// Returns `None` (status unknown) for "unknown" and any state not listed.
fn instance_status(node: &UksNode) -> Option<InstanceStatus> {
    let (state, error_info) = match node.state.as_str() {
        "running" => (InstanceState::InstanceRunning, None),
        "pending" => (InstanceState::InstanceCreating, None),
        "terminating" | "pending-removal" => (InstanceState::InstanceDeleting, None),
        // Reported as a failed creation so the autoscaler removes the node
        // and backs off from the group.
        "failed" => (
            InstanceState::InstanceCreating,
            Some(InstanceErrorInfo {
                error_code: "upcloud-node-failed".into(),
                error_message: format!("UKS reports node '{}' as failed", node.name),
                instance_error_class: OTHER_ERROR_CLASS,
            }),
        ),
        _ => return None,
    };

    Some(InstanceStatus {
        instance_state: state as i32,
        error_info,
    })
}

#[tonic::async_trait]
impl CloudProvider for UpcloudProvider {
    async fn node_groups(
        &self,
        _request: Request<NodeGroupsRequest>,
    ) -> Result<Response<NodeGroupsResponse>, Status> {
        let cached = self
            .node_groups
            .read()
            .map_err(|_| Status::internal("node group cache lock poisoned"))?;

        let node_groups = cached
            .iter()
            .map(|group| self.to_node_group(group))
            .collect();

        Ok(Response::new(NodeGroupsResponse { node_groups }))
    }

    async fn node_group_increase_size(
        &self,
        request: Request<NodeGroupIncreaseSizeRequest>,
    ) -> Result<Response<NodeGroupIncreaseSizeResponse>, Status> {
        let NodeGroupIncreaseSizeRequest { delta, id } = request.into_inner();
        if delta <= 0 {
            return Err(Status::invalid_argument(format!(
                "delta must be positive, got {delta}"
            )));
        }

        // Check everything against the cache, and release the lock before the
        // API call.
        let new_count = {
            let cached = self
                .node_groups
                .read()
                .map_err(|_| Status::internal("node group cache lock poisoned"))?;

            let group = cached
                .iter()
                .find(|group| group.name == id)
                .ok_or_else(|| Status::not_found(format!("node group '{id}' not found")))?;

            if group.state != "running" {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}' is '{}', not 'running'",
                    group.state
                )));
            }

            let new_count = group.count + delta;
            if new_count > self.group_max {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}': {new_count} nodes would exceed the group max {}",
                    self.group_max
                )));
            }

            let cluster_total: i32 = cached.iter().map(|group| group.count).sum::<i32>() + delta;
            if cluster_total > self.plan_limit {
                return Err(Status::failed_precondition(format!(
                    "{cluster_total} nodes in the cluster would exceed the plan limit {}",
                    self.plan_limit
                )));
            }

            new_count
        };

        self.api
            .set_node_group_count(&id, new_count)
            .await
            .map_err(|e| Status::unavailable(format!("resizing node group '{id}': {e}")))?;

        // Update the cache now so the autoscaler sees the new target (and no
        // second resize starts) before the next refresh.
        let mut cached = self
            .node_groups
            .write()
            .map_err(|_| Status::internal("node group cache lock poisoned"))?;
        if let Some(group) = cached.iter_mut().find(|group| group.name == id) {
            group.count = new_count;
            group.state = "scaling-up".into();
        }

        Ok(Response::new(NodeGroupIncreaseSizeResponse {}))
    }

    /// Lowers the target for nodes that were requested but never came up.
    /// Must never remove a running node, so the new count can't go below the
    /// number of running nodes.
    async fn node_group_decrease_target_size(
        &self,
        request: Request<NodeGroupDecreaseTargetSizeRequest>,
    ) -> Result<Response<NodeGroupDecreaseTargetSizeResponse>, Status> {
        let NodeGroupDecreaseTargetSizeRequest { delta, id } = request.into_inner();
        if delta >= 0 {
            return Err(Status::invalid_argument(format!(
                "delta must be negative, got {delta}"
            )));
        }

        // Check everything against the cache, and release the lock before the
        // API call.
        let new_count = {
            let cached = self
                .node_groups
                .read()
                .map_err(|_| Status::internal("node group cache lock poisoned"))?;

            let group = cached
                .iter()
                .find(|group| group.name == id)
                .ok_or_else(|| Status::not_found(format!("node group '{id}' not found")))?;

            // Nodes stuck coming up usually leave the group in "scaling-up" or
            // "failed", so those are allowed as well as "running".
            if !matches!(group.state.as_str(), "running" | "scaling-up" | "failed") {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}' is '{}'; can't decrease its target",
                    group.state
                )));
            }

            let new_count = group.count + delta;
            let running = group.nodes.iter().filter(|n| n.state == "running").count() as i32;
            if new_count < running {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}': target {new_count} is below its {running} running nodes"
                )));
            }
            if new_count < MIN_NODES_PER_GROUP {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}': target {new_count} is below the minimum {MIN_NODES_PER_GROUP}"
                )));
            }

            new_count
        };

        self.api
            .set_node_group_count(&id, new_count)
            .await
            .map_err(|e| Status::unavailable(format!("resizing node group '{id}': {e}")))?;

        // Update the cache now so the autoscaler sees the new target before
        // the next refresh.
        let mut cached = self
            .node_groups
            .write()
            .map_err(|_| Status::internal("node group cache lock poisoned"))?;
        if let Some(group) = cached.iter_mut().find(|group| group.name == id) {
            group.count = new_count;
            group.state = "scaling-down".into();
        }

        Ok(Response::new(NodeGroupDecreaseTargetSizeResponse {}))
    }

    /// Deletes the specific nodes the autoscaler chose (already drained).
    async fn node_group_delete_nodes(
        &self,
        request: Request<NodeGroupDeleteNodesRequest>,
    ) -> Result<Response<NodeGroupDeleteNodesResponse>, Status> {
        let NodeGroupDeleteNodesRequest { nodes, id } = request.into_inner();

        // Check everything against the cache and work out each node's UKS
        // name, before deleting anything. The lock is released before the
        // API calls.
        let targets: Vec<(String, String)> = {
            let cached = self
                .node_groups
                .read()
                .map_err(|_| Status::internal("node group cache lock poisoned"))?;

            let group = cached
                .iter()
                .find(|group| group.name == id)
                .ok_or_else(|| Status::not_found(format!("node group '{id}' not found")))?;

            // The autoscaler also deletes nodes that failed to come up, which
            // can leave the group in "scaling-up" or "failed".
            if !matches!(group.state.as_str(), "running" | "scaling-up" | "failed") {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}' is '{}'; can't delete nodes",
                    group.state
                )));
            }

            let new_count = group.count - nodes.len() as i32;
            if new_count < MIN_NODES_PER_GROUP {
                return Err(Status::failed_precondition(format!(
                    "node group '{id}': deleting {} nodes would leave {new_count}, below the minimum {MIN_NODES_PER_GROUP}",
                    nodes.len()
                )));
            }

            nodes
                .iter()
                .map(|node| {
                    let uks_node = node
                        .provider_id
                        .strip_prefix(PROVIDER_ID_PREFIX)
                        .and_then(|uuid| group.nodes.iter().find(|n| n.uuid == uuid))
                        .ok_or_else(|| {
                            Status::invalid_argument(format!(
                                "node '{}' ({}) is not in node group '{id}'",
                                node.name, node.provider_id
                            ))
                        })?;
                    Ok((uks_node.uuid.clone(), uks_node.name.clone()))
                })
                .collect::<Result<_, Status>>()?
        };

        // Delete one at a time; stop at the first failure.
        let mut deleted = Vec::new();
        let mut result = Ok(());
        for (uuid, name) in &targets {
            match self.api.delete_node(&id, name).await {
                Ok(()) => deleted.push(uuid),
                Err(e) => {
                    result = Err(Status::unavailable(format!(
                        "deleting node '{name}' from node group '{id}': {e}"
                    )));
                    break;
                }
            }
        }

        // Record what was deleted, even if a later delete failed, so the
        // autoscaler sees those nodes as going before the next refresh.
        if !deleted.is_empty() {
            let mut cached = self
                .node_groups
                .write()
                .map_err(|_| Status::internal("node group cache lock poisoned"))?;
            if let Some(group) = cached.iter_mut().find(|group| group.name == id) {
                group.count -= deleted.len() as i32;
                group.state = "scaling-down".into();
                for node in group.nodes.iter_mut() {
                    if deleted.contains(&&node.uuid) {
                        node.state = "terminating".into();
                    }
                }
            }
        }

        result?;
        Ok(Response::new(NodeGroupDeleteNodesResponse {}))
    }

    async fn node_group_for_node(
        &self,
        request: Request<NodeGroupForNodeRequest>,
    ) -> Result<Response<NodeGroupForNodeResponse>, Status> {
        // An empty NodeGroup (id "") tells the autoscaler the node isn't managed.
        let unmanaged = || {
            Ok(Response::new(NodeGroupForNodeResponse {
                node_group: Some(NodeGroup::default()),
            }))
        };

        let node = request.into_inner().node.unwrap_or_default();
        let Some(uuid) = node.provider_id.strip_prefix(PROVIDER_ID_PREFIX) else {
            return unmanaged();
        };

        let cached = self
            .node_groups
            .read()
            .map_err(|_| Status::internal("node group cache lock poisoned"))?;

        let node_group = cached
            .iter()
            .find(|group| group.nodes.iter().any(|n| n.uuid == uuid))
            .map(|group| self.to_node_group(group));

        match node_group {
            Some(node_group) => Ok(Response::new(NodeGroupForNodeResponse {
                node_group: Some(node_group),
            })),
            None => unmanaged(),
        }
    }

    async fn node_group_target_size(
        &self,
        request: Request<NodeGroupTargetSizeRequest>,
    ) -> Result<Response<NodeGroupTargetSizeResponse>, Status> {
        let id = request.into_inner().id;

        let cached = self
            .node_groups
            .read()
            .map_err(|_| Status::internal("node group cache lock poisoned"))?;

        let group = cached
            .iter()
            .find(|group| group.name == id)
            .ok_or_else(|| Status::not_found(format!("node group '{id}' not found")))?;

        Ok(Response::new(NodeGroupTargetSizeResponse {
            target_size: group.count,
        }))
    }

    async fn refresh(
        &self,
        _request: Request<RefreshRequest>,
    ) -> Result<Response<RefreshResponse>, Status> {
        let groups = self
            .api
            .fetch_node_groups_with_nodes()
            .await
            .map_err(|e| Status::unavailable(format!("fetching node groups: {e}")))?;

        *self
            .node_groups
            .write()
            .map_err(|_| Status::internal("node group cache lock poisoned"))? = groups;

        Ok(Response::new(RefreshResponse {}))
    }

    async fn cleanup(
        &self,
        _request: Request<CleanupRequest>,
    ) -> Result<Response<CleanupResponse>, Status> {
        Ok(Response::new(CleanupResponse {}))
    }

    async fn node_group_nodes(
        &self,
        request: Request<NodeGroupNodesRequest>,
    ) -> Result<Response<NodeGroupNodesResponse>, Status> {
        let id = request.into_inner().id;

        let cached = self
            .node_groups
            .read()
            .map_err(|_| Status::internal("node group cache lock poisoned"))?;

        let group = cached
            .iter()
            .find(|group| group.name == id)
            .ok_or_else(|| Status::not_found(format!("node group '{id}' not found")))?;

        let instances = group
            .nodes
            .iter()
            .map(|node| Instance {
                id: format!("{PROVIDER_ID_PREFIX}{}", node.uuid),
                status: instance_status(node),
            })
            .collect();

        Ok(Response::new(NodeGroupNodesResponse { instances }))
    }
}
