use std::collections::HashSet;

use async_trait::async_trait;
use ractor::{Actor, ActorId, ActorProcessingErr, ActorRef, RpcReplyPort, SupervisionEvent};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use malachitebft_metrics::Metrics;

#[derive(Debug)]
pub enum NodeMsg {
    RegisterSafetyCriticalChild(ActorId, RpcReplyPort<()>),
    SafetyFailure(String),
}

pub type NodeRef = ActorRef<NodeMsg>;

pub struct Node {
    metrics: Metrics,
    span: tracing::Span,
}

pub struct NodeState {
    safety_critical_children: HashSet<ActorId>,
    safety_hang: bool,
    shutting_down: bool,
}

impl Node {
    pub fn new(metrics: Metrics, span: tracing::Span) -> Self {
        Self { metrics, span }
    }

    pub async fn spawn(self) -> Result<(NodeRef, JoinHandle<()>), ractor::SpawnErr> {
        Actor::spawn(None, self, ()).await
    }

    fn enter_safety_hang(&self, myself: &NodeRef, state: &mut NodeState, reason: String) {
        if state.safety_hang {
            warn!(%reason, "Additional safety failure while node is already safety-hung");
            return;
        }

        error!(%reason, "Safety-critical failure; entering safety hang");
        state.safety_hang = true;
        self.metrics.set_safety_failure();
        myself.get_cell().stop_children(Some(reason));
    }
}

#[async_trait]
impl Actor for Node {
    type Msg = NodeMsg;
    type State = NodeState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: NodeRef,
        _args: (),
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(NodeState {
            safety_critical_children: HashSet::new(),
            safety_hang: false,
            shutting_down: false,
        })
    }

    #[tracing::instrument(name = "node", parent = &self.span, skip_all)]
    async fn handle(
        &self,
        myself: NodeRef,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            NodeMsg::RegisterSafetyCriticalChild(actor, reply_to) => {
                state.safety_critical_children.insert(actor);
                let _ = reply_to.send(());
                Ok(())
            }
            NodeMsg::SafetyFailure(reason) => {
                self.enter_safety_hang(&myself, state, reason);
                Ok(())
            }
        }
    }

    #[tracing::instrument(name = "node", parent = &self.span, skip_all)]
    async fn handle_supervisor_evt(
        &self,
        myself: NodeRef,
        evt: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match evt {
            SupervisionEvent::ActorStarted(cell) => {
                info!(actor = %cell.get_id(), "Actor has started");
            }
            SupervisionEvent::ActorTerminated(cell, _state, reason) => {
                if state.safety_hang || state.shutting_down {
                    info!(actor = %cell.get_id(), "Actor terminated during node shutdown");
                    return Ok(());
                }

                let reason = reason.unwrap_or_default();
                if state.safety_critical_children.contains(&cell.get_id()) {
                    self.enter_safety_hang(
                        &myself,
                        state,
                        format!(
                            "safety-critical actor terminated: {} ({reason})",
                            cell.get_id()
                        ),
                    );
                    return Ok(());
                }

                warn!(actor = %cell.get_id(), %reason, "Actor terminated unexpectedly");
                state.shutting_down = true;
                myself.stop(Some(format!(
                    "child actor terminated: {} ({reason})",
                    cell.get_id()
                )));
            }
            SupervisionEvent::ActorFailed(cell, error) => {
                if state.safety_hang || state.shutting_down {
                    info!(actor = %cell.get_id(), %error, "Actor failed during node shutdown");
                    return Ok(());
                }

                if state.safety_critical_children.contains(&cell.get_id()) {
                    self.enter_safety_hang(
                        &myself,
                        state,
                        format!("safety-critical actor failed: {} ({error})", cell.get_id()),
                    );
                    return Ok(());
                }

                error!(actor = %cell.get_id(), %error, "Actor failed unexpectedly");
                state.shutting_down = true;
                myself.stop(Some(format!(
                    "child actor failed: {} ({error})",
                    cell.get_id()
                )));
            }
            SupervisionEvent::ProcessGroupChanged(_) => (),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use malachitebft_metrics::{Metrics, SharedRegistry};
    use ractor::{Actor, ActorProcessingErr, ActorRef};
    use tokio::task::JoinHandle;

    use super::{Node, NodeMsg, NodeRef};

    struct Child;

    enum ChildMsg {
        Fail,
    }

    #[async_trait]
    impl Actor for Child {
        type Msg = ChildMsg;
        type State = ();
        type Arguments = ();

        async fn pre_start(
            &self,
            _myself: ActorRef<Self::Msg>,
            _args: (),
        ) -> Result<Self::State, ActorProcessingErr> {
            Ok(())
        }

        async fn handle(
            &self,
            _myself: ActorRef<Self::Msg>,
            msg: Self::Msg,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            match msg {
                ChildMsg::Fail => Err(eyre::eyre!("injected child failure").into()),
            }
        }
    }

    async fn spawn_node_with_child() -> (NodeRef, JoinHandle<()>, ActorRef<ChildMsg>, Metrics) {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, handle) = Node::new(metrics.clone(), tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        let (child, _) = Actor::spawn(None, Child, ()).await.expect("spawn child");
        child.link(node.get_cell());

        (node, handle, child, metrics)
    }

    #[tokio::test]
    async fn safety_failure_stops_children_but_keeps_node_alive() {
        let (node, mut handle, child, _metrics) = spawn_node_with_child().await;

        node.cast(NodeMsg::SafetyFailure("injected WAL failure".to_owned()))
            .expect("cast safety failure");
        child
            .wait(Some(Duration::from_secs(2)))
            .await
            .expect("safety hang stops child");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "node must remain alive in safety hang"
        );

        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");
    }

    #[tokio::test]
    async fn repeated_safety_failure_is_idempotent() {
        let (node, mut handle, child, metrics) = spawn_node_with_child().await;

        node.cast(NodeMsg::SafetyFailure("first failure".to_owned()))
            .expect("cast first safety failure");
        child
            .wait(Some(Duration::from_secs(2)))
            .await
            .expect("first safety failure stops child");
        node.cast(NodeMsg::SafetyFailure("second failure".to_owned()))
            .expect("cast second safety failure");
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(metrics.node_safety_failure.get(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "repeated safety failures must not stop the node"
        );

        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");
    }

    #[tokio::test]
    async fn ordinary_child_failure_still_terminates_node() {
        let (_node, handle, child, metrics) = spawn_node_with_child().await;

        child.cast(ChildMsg::Fail).expect("cast child failure");
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node did not terminate after child failure")
            .expect("join terminated node");
        assert_eq!(metrics.node_safety_failure.get(), 0);
    }

    #[tokio::test]
    async fn safety_critical_child_failure_enters_safety_hang() {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, mut handle) = Node::new(metrics.clone(), tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        let (child, _) = Actor::spawn(None, Child, ()).await.expect("spawn child");

        ractor::call!(node, NodeMsg::RegisterSafetyCriticalChild, child.get_id())
            .expect("register safety-critical child");
        child.link(node.get_cell());
        child.cast(ChildMsg::Fail).expect("cast child failure");
        child
            .wait(Some(Duration::from_secs(2)))
            .await
            .expect("safety-critical child stopped");

        tokio::time::timeout(Duration::from_secs(2), async {
            while metrics.node_safety_failure.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("node did not enter safety hang");
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut handle)
                .await
                .is_err(),
            "safety-critical child failure must keep the node alive"
        );

        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");
    }

    #[tokio::test]
    async fn safety_failure_sets_the_node_gauge() {
        let (node, handle, child, metrics) = spawn_node_with_child().await;

        node.cast(NodeMsg::SafetyFailure("injected WAL failure".to_owned()))
            .expect("cast safety failure");
        child
            .wait(Some(Duration::from_secs(2)))
            .await
            .expect("safety hang stops child");
        assert_eq!(metrics.node_safety_failure.get(), 1);

        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");
    }
}
