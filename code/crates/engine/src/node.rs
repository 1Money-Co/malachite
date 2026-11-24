use std::collections::HashSet;

use async_trait::async_trait;
use ractor::{
    Actor, ActorCell, ActorId, ActorProcessingErr, ActorRef, ActorStatus, RpcReplyPort,
    SupervisionEvent,
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use malachitebft_metrics::Metrics;

#[derive(Debug)]
pub enum NodeMsg {
    CommitChildren(Vec<ChildRegistration>, RpcReplyPort<Result<(), String>>),
    SafetyFailure(String),
}

#[derive(Debug)]
pub struct ChildRegistration {
    name: &'static str,
    actor: ActorCell,
    safety_critical: bool,
    survives_safety_hang: bool,
}

impl ChildRegistration {
    pub fn ordinary(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: false,
            survives_safety_hang: false,
        }
    }

    pub fn safety_critical(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: true,
            survives_safety_hang: false,
        }
    }

    pub fn safety_hang_survivor(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: false,
            survives_safety_hang: true,
        }
    }
}

pub type NodeRef = ActorRef<NodeMsg>;

fn actor_is_active(actor: &ActorCell) -> bool {
    matches!(
        actor.get_status(),
        ActorStatus::Starting | ActorStatus::Running | ActorStatus::Upgrading
    )
}

pub struct Node {
    metrics: Metrics,
    span: tracing::Span,
}

pub struct NodeState {
    safety_critical_children: HashSet<ActorId>,
    safety_hang_survivors: HashSet<ActorId>,
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
        for child in myself.get_cell().get_children() {
            if !state.safety_hang_survivors.contains(&child.get_id()) {
                child.stop(Some(reason.clone()));
            }
        }
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
            safety_hang_survivors: HashSet::new(),
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
            NodeMsg::CommitChildren(children, reply_to) => {
                if state.safety_hang || state.shutting_down {
                    let _ = reply_to.send(Err("node is not accepting children".to_owned()));
                    return Ok(());
                }

                if let Some(child) = children.iter().find(|child| !actor_is_active(&child.actor)) {
                    let _ = reply_to.send(Err(format!("{} actor is not running", child.name)));
                    return Ok(());
                }

                for child in &children {
                    if child.safety_critical {
                        state.safety_critical_children.insert(child.actor.get_id());
                    }
                    if child.survives_safety_hang {
                        state.safety_hang_survivors.insert(child.actor.get_id());
                    }
                    child.actor.link(myself.get_cell());
                }

                if let Some(child) = children.iter().find(|child| !actor_is_active(&child.actor)) {
                    let error = format!("{} actor is not running", child.name);
                    for child in &children {
                        child.actor.unlink(myself.get_cell());
                        state.safety_critical_children.remove(&child.actor.get_id());
                        state.safety_hang_survivors.remove(&child.actor.get_id());
                    }
                    let _ = reply_to.send(Err(error));
                    return Ok(());
                }

                let _ = reply_to.send(Ok(()));
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

    use super::{ChildRegistration, Node, NodeMsg, NodeRef};

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
    async fn safety_hang_preserves_registered_survivor() {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, mut handle) = Node::new(metrics, tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        let (stopped_child, _) = Actor::spawn(None, Child, ())
            .await
            .expect("spawn stopped child");
        let (survivor, _) = Actor::spawn(None, Child, ()).await.expect("spawn survivor");
        ractor::call!(
            node,
            NodeMsg::CommitChildren,
            vec![
                ChildRegistration::ordinary("runtime child", stopped_child.get_cell()),
                ChildRegistration::safety_hang_survivor(
                    "application connector",
                    survivor.get_cell()
                ),
            ]
        )
        .expect("commit child roles")
        .expect("children are active");

        node.cast(NodeMsg::SafetyFailure("injected WAL failure".to_owned()))
            .expect("cast safety failure");
        stopped_child
            .wait(Some(Duration::from_secs(2)))
            .await
            .expect("runtime child stopped");
        assert_eq!(survivor.get_status(), ractor::ActorStatus::Running);
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
        survivor
            .wait(Some(Duration::from_secs(2)))
            .await
            .expect("explicit node stop must stop survivor");
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

        ractor::call!(
            node,
            NodeMsg::CommitChildren,
            vec![ChildRegistration::safety_critical(
                "test child",
                child.get_cell()
            )]
        )
        .expect("commit safety-critical child")
        .expect("safety-critical child is running");
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
    async fn commit_rejects_a_child_that_is_already_stopped() {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, handle) = Node::new(metrics, tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        let (child, _) = Actor::spawn(None, Child, ()).await.expect("spawn child");
        child
            .stop_and_wait(None, None)
            .await
            .expect("stop child before commit");

        let result = ractor::call!(
            node,
            NodeMsg::CommitChildren,
            vec![ChildRegistration::safety_critical("WAL", child.get_cell())]
        )
        .expect("commit RPC failed");
        assert_eq!(result, Err("WAL actor is not running".to_owned()));

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
