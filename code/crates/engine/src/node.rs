use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use ractor::{
    Actor, ActorCell, ActorId, ActorProcessingErr, ActorRef, ActorStatus, RpcReplyPort,
    SupervisionEvent,
};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use malachitebft_metrics::Metrics;

type PostLinkProbe = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

#[derive(Debug)]
pub enum NodeMsg {
    CommitChildren(Vec<ChildRegistration>, RpcReplyPort<Result<(), String>>),
    SafetyFailure(String),
}

pub struct ChildRegistration {
    name: &'static str,
    actor: ActorCell,
    safety_critical: bool,
    survives_safety_hang: bool,
    shutdown_first: bool,
    post_link_probe: Option<PostLinkProbe>,
}

impl fmt::Debug for ChildRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChildRegistration")
            .field("name", &self.name)
            .field("actor", &self.actor)
            .field("safety_critical", &self.safety_critical)
            .field("survives_safety_hang", &self.survives_safety_hang)
            .field("shutdown_first", &self.shutdown_first)
            .field("has_post_link_probe", &self.post_link_probe.is_some())
            .finish()
    }
}

impl ChildRegistration {
    pub fn ordinary(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: false,
            survives_safety_hang: false,
            shutdown_first: false,
            post_link_probe: None,
        }
    }

    pub fn shutdown_first(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: false,
            survives_safety_hang: false,
            shutdown_first: true,
            post_link_probe: None,
        }
    }

    pub fn safety_critical(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: true,
            survives_safety_hang: false,
            shutdown_first: false,
            post_link_probe: None,
        }
    }

    pub fn safety_critical_with_probe<F>(
        name: &'static str,
        actor: ActorCell,
        post_link_probe: F,
    ) -> Self
    where
        F: Future<Output = Result<(), String>> + Send + 'static,
    {
        Self {
            name,
            actor,
            safety_critical: true,
            survives_safety_hang: false,
            shutdown_first: false,
            post_link_probe: Some(Box::pin(post_link_probe)),
        }
    }

    pub fn safety_hang_survivor(name: &'static str, actor: ActorCell) -> Self {
        Self {
            name,
            actor,
            safety_critical: false,
            survives_safety_hang: true,
            shutdown_first: false,
            post_link_probe: None,
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

async fn stop_child_and_wait(actor: &ActorCell) -> Result<(), ActorProcessingErr> {
    match actor.get_status() {
        ActorStatus::Stopped => Ok(()),
        ActorStatus::Draining | ActorStatus::Stopping => {
            actor.wait(None).await?;
            Ok(())
        }
        _ => {
            actor.stop_and_wait(None, None).await?;
            Ok(())
        }
    }
}

pub struct Node {
    metrics: Metrics,
    span: tracing::Span,
}

pub struct NodeState {
    shutdown_first_children: HashSet<ActorId>,
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
            shutdown_first_children: HashSet::new(),
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
            NodeMsg::CommitChildren(mut children, reply_to) => {
                if state.safety_hang || state.shutting_down {
                    let _ = reply_to.send(Err("node is not accepting children".to_owned()));
                    return Ok(());
                }

                if let Some(child) = children.iter().find(|child| !actor_is_active(&child.actor)) {
                    let _ = reply_to.send(Err(format!("{} actor is not running", child.name)));
                    return Ok(());
                }

                for child in &children {
                    if child.shutdown_first {
                        state.shutdown_first_children.insert(child.actor.get_id());
                    }
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
                    state.shutting_down = true;
                    for child in &children {
                        child.actor.unlink(myself.get_cell());
                        state.shutdown_first_children.remove(&child.actor.get_id());
                        state.safety_critical_children.remove(&child.actor.get_id());
                        state.safety_hang_survivors.remove(&child.actor.get_id());
                    }
                    let _ = reply_to.send(Err(error));
                    return Ok(());
                }

                for index in 0..children.len() {
                    let Some(probe) = children[index].post_link_probe.take() else {
                        continue;
                    };
                    if let Err(error) = probe.await {
                        let error =
                            format!("{} liveness probe failed: {error}", children[index].name);
                        state.shutting_down = true;
                        for child in &children {
                            child.actor.unlink(myself.get_cell());
                            state.shutdown_first_children.remove(&child.actor.get_id());
                            state.safety_critical_children.remove(&child.actor.get_id());
                            state.safety_hang_survivors.remove(&child.actor.get_id());
                        }
                        let _ = reply_to.send(Err(error));
                        return Ok(());
                    }
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
    async fn post_stop(
        &self,
        myself: NodeRef,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        let children = myself.get_cell().get_children();
        let mut first_error = None;
        for child in &children {
            if state.shutdown_first_children.contains(&child.get_id()) {
                if let Err(error) = stop_child_and_wait(child).await {
                    error!(actor = %child.get_id(), %error, "Failed to stop shutdown-first child");
                    first_error.get_or_insert(error);
                }
            }
        }
        for child in &children {
            if state.safety_critical_children.contains(&child.get_id()) {
                if let Err(error) = stop_child_and_wait(child).await {
                    error!(actor = %child.get_id(), %error, "Failed to stop safety-critical child");
                    first_error.get_or_insert(error);
                }
            }
        }
        myself
            .stop_children_and_wait(None, Some(Duration::from_secs(1)))
            .await;
        first_error.map_or(Ok(()), Err)
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
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use malachitebft_metrics::{Metrics, SharedRegistry};
    use ractor::{Actor, ActorProcessingErr, ActorRef};
    use tokio::task::JoinHandle;

    use super::{ChildRegistration, Node, NodeMsg, NodeRef};

    struct Child;

    struct ShutdownChild {
        name: &'static str,
        order: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    #[async_trait]
    impl Actor for ShutdownChild {
        type Msg = ();
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
            _msg: Self::Msg,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            Ok(())
        }

        async fn post_stop(
            &self,
            _myself: ActorRef<Self::Msg>,
            _state: &mut Self::State,
        ) -> Result<(), ActorProcessingErr> {
            self.order.lock().unwrap().push(self.name);
            if self.fail {
                Err(eyre::eyre!("injected {} shutdown failure", self.name).into())
            } else {
                Ok(())
            }
        }
    }

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

    #[tokio::test]
    async fn shutdown_stops_consensus_and_sync_before_wal() {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, handle) = Node::new(metrics, tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        let order = Arc::new(Mutex::new(Vec::new()));
        let (consensus, _) = Actor::spawn(
            None,
            ShutdownChild {
                name: "consensus",
                order: order.clone(),
                fail: false,
            },
            (),
        )
        .await
        .expect("spawn consensus");
        let (sync, _) = Actor::spawn(
            None,
            ShutdownChild {
                name: "sync",
                order: order.clone(),
                fail: false,
            },
            (),
        )
        .await
        .expect("spawn sync");
        let (wal, _) = Actor::spawn(
            None,
            ShutdownChild {
                name: "WAL",
                order: order.clone(),
                fail: false,
            },
            (),
        )
        .await
        .expect("spawn WAL");

        ractor::call!(
            node,
            NodeMsg::CommitChildren,
            vec![
                ChildRegistration::shutdown_first("consensus", consensus.get_cell()),
                ChildRegistration::shutdown_first("sync", sync.get_cell()),
                ChildRegistration::safety_critical("WAL", wal.get_cell()),
            ]
        )
        .expect("commit child roles")
        .expect("children are active");
        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");

        let order = order.lock().unwrap();
        let consensus = order.iter().position(|name| *name == "consensus").unwrap();
        let sync = order.iter().position(|name| *name == "sync").unwrap();
        let wal = order.iter().position(|name| *name == "WAL").unwrap();
        assert!(consensus < wal, "shutdown order was {order:?}");
        assert!(sync < wal, "shutdown order was {order:?}");
    }

    #[tokio::test]
    async fn shutdown_continues_after_child_stop_failure() {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, handle) = Node::new(metrics, tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        let order = Arc::new(Mutex::new(Vec::new()));
        let (consensus, _) = Actor::spawn(
            None,
            ShutdownChild {
                name: "consensus",
                order: order.clone(),
                fail: true,
            },
            (),
        )
        .await
        .expect("spawn consensus");
        let (wal, _) = Actor::spawn(
            None,
            ShutdownChild {
                name: "WAL",
                order: order.clone(),
                fail: false,
            },
            (),
        )
        .await
        .expect("spawn WAL");

        ractor::call!(
            node,
            NodeMsg::CommitChildren,
            vec![
                ChildRegistration::shutdown_first("consensus", consensus.get_cell()),
                ChildRegistration::safety_critical("WAL", wal.get_cell()),
            ]
        )
        .expect("commit child roles")
        .expect("children are active");
        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");

        let order = order.lock().unwrap();
        assert_eq!(order.as_slice(), &["consensus", "WAL"]);
    }
}
