use std::future::{pending, Future};
use std::{error::Error, fmt};

use ractor::ActorProcessingErr;
use tracing::error;

use crate::node::{NodeMsg, NodeRef};

#[derive(Debug)]
struct LivenessFailure(String);

impl fmt::Display for LivenessFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "liveness failure: {}", self.0)
    }
}

impl Error for LivenessFailure {}

pub fn is_liveness_failure(error: &ActorProcessingErr) -> bool {
    error.downcast_ref::<LivenessFailure>().is_some()
}

pub async fn stop_on_failure<A, E>(
    operation: impl Future<Output = Result<A, E>>,
    format_error: impl FnOnce(E) -> String,
) -> Result<A, ActorProcessingErr> {
    match operation.await {
        Ok(value) => Ok(value),
        Err(error) => {
            let reason = format_error(error);
            error!(%reason, "Liveness failure; failing actor to trigger restart");
            Err(Box::new(LivenessFailure(reason)))
        }
    }
}

pub async fn hang_on_safety_failure<A, E>(
    node: &NodeRef,
    operation: impl Future<Output = Result<A, E>>,
    format_error: impl FnOnce(E) -> String,
) -> A {
    match operation.await {
        Ok(value) => value,
        Err(error) => {
            let reason = format_error(error);
            error!(%reason, "Safety-critical operation failed; stopping consensus progress");
            let _ = node.cast(NodeMsg::SafetyFailure(reason));
            pending::<A>().await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use malachitebft_metrics::{Metrics, SharedRegistry};

    use crate::node::Node;

    use super::hang_on_safety_failure;

    async fn spawn_node() -> (crate::node::NodeRef, tokio::task::JoinHandle<()>, Metrics) {
        let registry = SharedRegistry::new(Default::default(), None);
        let metrics = Metrics::register(&registry);
        let (node, handle) = Node::new(metrics.clone(), tracing::Span::none())
            .spawn()
            .await
            .expect("spawn node");
        (node, handle, metrics)
    }

    #[tokio::test]
    async fn safety_failure_is_reported_before_the_operation_hangs() {
        let (node, handle, metrics) = spawn_node().await;
        let task = tokio::spawn({
            let node = node.clone();
            async move {
                hang_on_safety_failure(&node, async { Err::<(), _>("disk full") }, |error| {
                    format!("WAL flush failed: {error}")
                })
                .await
            }
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while metrics.node_safety_failure.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("node did not receive safety failure");
        assert!(!task.is_finished(), "failed operation must remain pending");

        task.abort();
        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");
    }

    #[tokio::test]
    async fn successful_operation_returns_unchanged() {
        let (node, handle, metrics) = spawn_node().await;

        let value = hang_on_safety_failure(&node, async { Ok::<_, &str>(42_u32) }, |_| {
            "unexpected".to_owned()
        })
        .await;

        assert_eq!(value, 42);
        assert_eq!(metrics.node_safety_failure.get(), 0);

        node.stop(None);
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("node cleanup timeout")
            .expect("join node cleanup");
    }
}
