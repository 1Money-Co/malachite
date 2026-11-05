//! Implementation of a host actor for bridiging consensus and the application via a set of channels.

use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, SpawnErr};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{error, warn};

use malachitebft_engine::host::HostMsg;

use crate::app::metrics::Metrics;
use crate::app::types::core::Context;
use crate::msgs::AppMsg;

/// Actor for bridging consensus and the application via a set of channels.
///
/// This actor is responsible for forwarding messages from the
/// consensus actor to the application over a channel, and vice-versa.
pub struct Connector<Ctx>
where
    Ctx: Context,
{
    sender: mpsc::Sender<AppMsg<Ctx>>,

    // TODO: add some metrics
    #[allow(dead_code)]
    metrics: Metrics,
}

impl<Ctx> Connector<Ctx>
where
    Ctx: Context,
{
    pub fn new(sender: mpsc::Sender<AppMsg<Ctx>>, metrics: Metrics) -> Self {
        Connector { sender, metrics }
    }

    pub async fn spawn(
        sender: mpsc::Sender<AppMsg<Ctx>>,
        metrics: Metrics,
    ) -> Result<ActorRef<HostMsg<Ctx>>, SpawnErr>
    where
        Ctx: Context,
    {
        let (actor_ref, _) = Actor::spawn(None, Self::new(sender, metrics), ()).await?;
        Ok(actor_ref)
    }
}

impl<Ctx> Connector<Ctx>
where
    Ctx: Context,
{
    async fn handle_msg(
        &self,
        _myself: ActorRef<HostMsg<Ctx>>,
        msg: HostMsg<Ctx>,
        _state: &mut (),
    ) -> Result<(), ActorProcessingErr> {
        match msg {
            HostMsg::ConsensusReady { reply_to } => {
                let (reply, rx) = oneshot::channel();
                self.sender.send(AppMsg::ConsensusReady { reply }).await?;

                let (start_height, validator_set) = rx.await?;
                reply_to.send((start_height, validator_set))?;
            }

            HostMsg::StartedRound {
                height,
                round,
                proposer,
                role,
                reply_to,
            } => {
                let (reply_value, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::StartedRound {
                        height,
                        round,
                        proposer,
                        role,
                        reply_value,
                    })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::GetValue {
                height,
                round,
                timeout,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetValue {
                        height,
                        round,
                        timeout,
                        reply,
                    })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::ExtendVote {
                height,
                round,
                value_id,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::ExtendVote {
                        height,
                        round,
                        value_id,
                        reply,
                    })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::VerifyVoteExtension {
                height,
                round,
                value_id,
                extension,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::VerifyVoteExtension {
                        height,
                        round,
                        value_id,
                        extension,
                        reply,
                    })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::RestreamValue {
                height,
                round,
                valid_round,
                address,
                value_id,
            } => {
                self.sender
                    .send(AppMsg::RestreamProposal {
                        height,
                        round,
                        valid_round,
                        address,
                        value_id,
                    })
                    .await?
            }

            HostMsg::GetHistoryMinHeight { reply_to } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetHistoryMinHeight { reply })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::ReceivedProposalPart {
                from,
                part,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::ReceivedProposalPart { from, part, reply })
                    .await?;

                if let Some(value) = rx.await? {
                    reply_to.send(value)?;
                }
            }

            HostMsg::Decided {
                certificate,
                extensions,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::Decided {
                        certificate,
                        extensions,
                        reply,
                    })
                    .await?;

                // Do not block processing of other messages while waiting for the next height
                tokio::spawn(async move {
                    match rx.await {
                        Ok(next) => {
                            if let Err(e) = reply_to.send(next) {
                                error!("Failed to send next height and validator set: {e}");
                            }
                        }
                        Err(e) => error!("Application dropped the decided reply: {e}"),
                    }
                });
            }

            HostMsg::GetDecidedValue { height, reply_to } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::GetDecidedValue { height, reply })
                    .await?;

                reply_to.send(rx.await?)?;
            }

            HostMsg::ProcessSyncedValue {
                height,
                round,
                proposer,
                value_bytes,
                reply_to,
            } => {
                let (reply, rx) = oneshot::channel();

                self.sender
                    .send(AppMsg::ProcessSyncedValue {
                        height,
                        round,
                        proposer,
                        value_bytes,
                        reply,
                    })
                    .await?;

                if let Some(value) = rx.await? {
                    if let Err(e) = reply_to.send(value) {
                        error!("Failed to send processed synced value: {e}");
                    }
                } else {
                    warn!("Failed to decode synced value");
                }
            }
        };

        Ok(())
    }
}

#[async_trait]
impl<Ctx> Actor for Connector<Ctx>
where
    Ctx: Context,
{
    type Msg = HostMsg<Ctx>;
    type State = ();
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(())
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        msg: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        if let Err(e) = self.handle_msg(myself, msg, state).await {
            tracing::error!("Error processing message: {e}");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use malachitebft_engine::host::HostMsg;
    use malachitebft_test::{Height, TestContext, ValueId};
    use ractor::rpc::CallResult;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tracing::field::{Field, Visit};
    use tracing::{Event, Level, Subscriber};
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};
    use tracing_subscriber::Layer;

    use super::Connector;
    use crate::app::metrics::Metrics;
    use crate::app::types::core::{CommitCertificate, Round, VoteExtensions};
    use crate::msgs::AppMsg;

    #[derive(Clone, Default)]
    struct EventRecorder(Arc<Mutex<Vec<(Level, String)>>>);

    impl EventRecorder {
        fn contains_error(&self, needle: &str) -> bool {
            self.0
                .lock()
                .expect("event recorder lock poisoned")
                .iter()
                .any(|(level, message)| *level == Level::ERROR && message.contains(needle))
        }
    }

    impl<S> Layer<S> for EventRecorder
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _ctx: LayerContext<'_, S>) {
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            if let Some(message) = visitor.message {
                self.0
                    .lock()
                    .expect("event recorder lock poisoned")
                    .push((*event.metadata().level(), message));
            }
        }
    }

    #[derive(Default)]
    struct MessageVisitor {
        message: Option<String>,
    }

    impl Visit for MessageVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            if field.name() == "message" {
                self.message = Some(format!("{value:?}"));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn decided_reply_is_async_and_logs_when_application_drops_it() {
        let recorder = EventRecorder::default();
        let subscriber = tracing_subscriber::registry().with(recorder.clone());
        let _subscriber_guard = tracing::subscriber::set_default(subscriber);

        let (app_tx, mut app_rx) = mpsc::channel(2);
        let connector = Connector::<TestContext>::spawn(app_tx, Metrics::default())
            .await
            .expect("connector should spawn");

        let decided_connector = connector.clone();
        let decided_call = tokio::spawn(async move {
            decided_connector
                .call(
                    |reply_to| HostMsg::Decided {
                        certificate: CommitCertificate::new(
                            Height::new(7),
                            Round::new(2),
                            ValueId::new(7002),
                            Vec::new(),
                        ),
                        extensions: VoteExtensions::default(),
                        reply_to,
                    },
                    Some(Duration::from_secs(1)),
                )
                .await
        });

        let decided_reply = match timeout(Duration::from_secs(1), app_rx.recv())
            .await
            .expect("connector should forward Decided without blocking")
            .expect("application channel should remain open")
        {
            AppMsg::Decided { reply, .. } => reply,
            other => panic!("expected Decided, got {other:?}"),
        };

        let history_connector = connector.clone();
        let history_call = tokio::spawn(async move {
            history_connector
                .call(
                    |reply_to| HostMsg::GetHistoryMinHeight { reply_to },
                    Some(Duration::from_secs(1)),
                )
                .await
        });

        match timeout(Duration::from_secs(1), app_rx.recv())
            .await
            .expect("connector should process another message while Decided is pending")
            .expect("application channel should remain open")
        {
            AppMsg::GetHistoryMinHeight { reply } => {
                reply
                    .send(Height::new(3))
                    .expect("history reply receiver should remain open");
            }
            other => panic!("expected GetHistoryMinHeight, got {other:?}"),
        }

        let history_result = timeout(Duration::from_secs(1), history_call)
            .await
            .expect("history request should complete")
            .expect("history task should not panic")
            .expect("history actor call should not fail");
        assert!(matches!(
            history_result,
            CallResult::Success(height) if height == Height::new(3)
        ));

        drop(decided_reply);
        timeout(Duration::from_secs(1), decided_call)
            .await
            .expect("dropped Decided reply should finish the pending call")
            .expect("Decided task should not panic")
            .expect("Decided actor call should not fail");
        assert!(recorder.contains_error("Application dropped the decided reply"));
    }
}
