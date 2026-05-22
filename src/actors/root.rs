use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::{Infallible, SendError};
use kameo::message::{Context, Message};

use crate::{Error, Result, StoreLocation};

use super::classifier;
use super::clock;
use super::decoder;
use super::dispatch;
use super::ingress;
use super::owner;
use super::pipeline::PipelineReply;
use super::policy;
use super::reply::{self, TextReply};
use super::state;
use super::store;
use super::subscription;
use super::trace::{ActorTrace, TraceAction, TraceNode};

const MIRROR_KIND_STAMPED_ENTRY: &str = "StampedEntry";

pub struct SpiritRoot {
    owner: ActorRef<owner::OwnerPlane>,
    ingress: ActorRef<ingress::IngressPhase>,
    dispatch: ActorRef<dispatch::DispatchPhase>,
    encoder: ActorRef<reply::ReplyTextEncoder>,
    store: ActorRef<store::RecordStore>,
    handover: HandoverState,
}

#[derive(Clone)]
pub struct Arguments {
    pub store: StoreLocation,
    pub bootstrap_policy_source: policy::BootstrapPolicySource,
}

pub struct SubmitText {
    pub text: String,
}

pub struct SubmitRequest {
    pub request: signal_persona_spirit::Operation,
}

pub struct SubmitFrameRequest {
    pub request: signal_frame::Request<signal_persona_spirit::Operation>,
}

pub struct SubmitOwnerRequest {
    pub request: owner_signal_persona_spirit::Operation,
}

pub struct SubmitUpgradeRequest {
    pub request: signal_version_handover::Operation,
}

#[derive(Debug, kameo::Reply)]
pub struct RootTextReply {
    text: String,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootOperationReply {
    reply: signal_persona_spirit::Reply,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootFrameReply {
    reply: signal_frame::Reply<signal_persona_spirit::Reply>,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootOwnerReply {
    reply: owner_signal_persona_spirit::Reply,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootUpgradeReply {
    reply: signal_version_handover::Reply,
    trace: ActorTrace,
}

pub struct SpiritActorRuntime {
    root: ActorRef<SpiritRoot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HandoverState {
    Active,
    HandoverMode {
        accepted_marker: signal_version_handover::HandoverMarker,
    },
    PrivateUpgradeOnly,
}

impl Arguments {
    pub fn new(store: StoreLocation) -> Self {
        Self {
            store,
            bootstrap_policy_source: policy::BootstrapPolicySource::default(),
        }
    }

    pub fn with_bootstrap_policy_source(
        store: StoreLocation,
        bootstrap_policy_source: policy::BootstrapPolicySource,
    ) -> Self {
        Self {
            store,
            bootstrap_policy_source,
        }
    }
}

impl SpiritRoot {
    fn new(
        owner: ActorRef<owner::OwnerPlane>,
        ingress: ActorRef<ingress::IngressPhase>,
        dispatch: ActorRef<dispatch::DispatchPhase>,
        encoder: ActorRef<reply::ReplyTextEncoder>,
        store: ActorRef<store::RecordStore>,
    ) -> Self {
        Self {
            owner,
            ingress,
            dispatch,
            encoder,
            store,
            handover: HandoverState::Active,
        }
    }

    pub async fn start(arguments: Arguments) -> Result<ActorRef<Self>> {
        let actor_reference = Self::spawn(arguments);
        actor_reference.wait_for_startup().await;
        Ok(actor_reference)
    }

    pub async fn stop(actor_reference: ActorRef<Self>) -> Result<()> {
        actor_reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        actor_reference.wait_for_shutdown().await;
        Ok(())
    }

    async fn submit_text(&self, text: String) -> Result<RootTextReply> {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReceived);
        let pipeline = self
            .ingress
            .ask(ingress::AcceptText { text, trace })
            .await
            .map_err(Self::pipeline_send_error)?;
        let encoded = self.encode(pipeline).await?;
        let mut trace = encoded.trace().clone();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReplied);
        Ok(RootTextReply::new(encoded.into_text(), trace))
    }

    async fn submit_request(
        &self,
        request: signal_persona_spirit::Operation,
    ) -> Result<RootOperationReply> {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReceived);
        let frame = self
            .dispatch
            .ask(dispatch::RouteRequest { request, trace })
            .await
            .map_err(Self::pipeline_send_error)?;
        let (reply, mut trace) = frame.into_parts();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReplied);
        Ok(RootOperationReply::new(reply, trace))
    }

    async fn submit_frame_request(
        &self,
        request: signal_frame::Request<signal_persona_spirit::Operation>,
    ) -> Result<RootFrameReply> {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReceived);
        let frame = self
            .dispatch
            .ask(dispatch::RouteFrameRequest { request, trace })
            .await
            .map_err(Self::frame_send_error)?;
        let (reply, mut trace) = frame.into_parts();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReplied);
        Ok(RootFrameReply::new(reply, trace))
    }

    async fn submit_owner_request(
        &self,
        request: owner_signal_persona_spirit::Operation,
    ) -> Result<RootOwnerReply> {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReceived);
        let owner = self
            .owner
            .ask(owner::RouteOwnerRequest { request, trace })
            .await
            .map_err(Self::owner_send_error)?;
        let mut trace = owner.trace;
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReplied);
        Ok(RootOwnerReply::new(owner.reply, trace))
    }

    async fn submit_upgrade_request(
        &mut self,
        request: signal_version_handover::Operation,
    ) -> Result<RootUpgradeReply> {
        let mut trace = ActorTrace::new();
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReceived);
        let reply = match request {
            signal_version_handover::Operation::AskHandoverMarker(request) => {
                let marker = self
                    .store
                    .ask(store::ReadHandoverMarker { request, trace })
                    .await
                    .map_err(Self::pipeline_send_error)?;
                trace = marker.trace;
                signal_version_handover::Reply::HandoverMarker(marker.marker)
            }
            signal_version_handover::Operation::ReadyToHandover(report) => {
                if matches!(self.handover, HandoverState::Active) {
                    let marker = self
                        .store
                        .ask(store::ReadHandoverMarker {
                            request: signal_version_handover::MarkerRequest {
                                component: report.component.clone(),
                            },
                            trace,
                        })
                        .await
                        .map_err(Self::pipeline_send_error)?;
                    trace = marker.trace;
                    if marker.marker.commit_sequence == report.source_marker.commit_sequence {
                        self.handover = HandoverState::HandoverMode {
                            accepted_marker: marker.marker.clone(),
                        };
                        signal_version_handover::Reply::HandoverAccepted(
                            signal_version_handover::HandoverAcceptance {
                                accepted_marker: marker.marker,
                            },
                        )
                    } else {
                        Self::handover_rejected(
                            report.component,
                            signal_version_handover::HandoverRejectionReason::CommitSequenceAdvanced,
                        )
                    }
                } else {
                    Self::handover_rejected(
                        report.component,
                        signal_version_handover::HandoverRejectionReason::AlreadyInHandover,
                    )
                }
            }
            signal_version_handover::Operation::HandoverCompleted(report) => {
                let accepted_marker = match &self.handover {
                    HandoverState::HandoverMode { accepted_marker } => {
                        Some(accepted_marker.clone())
                    }
                    HandoverState::Active | HandoverState::PrivateUpgradeOnly => None,
                };
                if accepted_marker.as_ref() != Some(&report.accepted_marker) {
                    Self::handover_rejected(
                        report.component,
                        signal_version_handover::HandoverRejectionReason::NotReady,
                    )
                } else {
                    let marker = self
                        .store
                        .ask(store::ReadHandoverMarker {
                            request: signal_version_handover::MarkerRequest {
                                component: report.component.clone(),
                            },
                            trace,
                        })
                        .await
                        .map_err(Self::pipeline_send_error)?;
                    trace = marker.trace;
                    if marker.marker.commit_sequence != report.accepted_marker.commit_sequence {
                        Self::handover_rejected(
                            report.component,
                            signal_version_handover::HandoverRejectionReason::CommitSequenceAdvanced,
                        )
                    } else {
                        self.handover = HandoverState::PrivateUpgradeOnly;
                        signal_version_handover::Reply::HandoverFinalized(
                            signal_version_handover::HandoverFinalization {
                                finalized_marker: report.accepted_marker,
                            },
                        )
                    }
                }
            }
            signal_version_handover::Operation::Mirror(payload) => {
                if !matches!(self.handover, HandoverState::PrivateUpgradeOnly) {
                    Self::handover_rejected(
                        payload.component,
                        signal_version_handover::HandoverRejectionReason::NotReady,
                    )
                } else if payload.kind.as_str() != MIRROR_KIND_STAMPED_ENTRY {
                    Self::handover_rejected(
                        payload.component,
                        signal_version_handover::HandoverRejectionReason::SchemaMismatch,
                    )
                } else {
                    match rkyv::from_bytes::<crate::store::StampedEntry, rkyv::rancor::Error>(
                        &payload.payload,
                    ) {
                        Ok(entry) => {
                            let component = payload.component.clone();
                            let captured = self
                                .store
                                .ask(store::CaptureEntry { entry, trace })
                                .await
                                .map_err(Self::pipeline_send_error)?;
                            let (_reply, capture_trace) = captured.into_parts();
                            let marker = self
                                .store
                                .ask(store::ReadHandoverMarker {
                                    request: signal_version_handover::MarkerRequest {
                                        component: component.clone(),
                                    },
                                    trace: capture_trace,
                                })
                                .await
                                .map_err(Self::pipeline_send_error)?;
                            trace = marker.trace;
                            signal_version_handover::Reply::MirrorAcknowledged(
                                signal_version_handover::MirrorAcknowledgement {
                                    component,
                                    write_counter: marker.marker.write_counter,
                                },
                            )
                        }
                        Err(_error) => Self::handover_rejected(
                            payload.component,
                            signal_version_handover::HandoverRejectionReason::SchemaMismatch,
                        ),
                    }
                }
            }
            signal_version_handover::Operation::Divergence(payload) => {
                signal_version_handover::Reply::DivergenceAcknowledged(
                    signal_version_handover::DivergenceAcknowledgement {
                        component: payload.component,
                        divergence_identifier: 0,
                    },
                )
            }
            signal_version_handover::Operation::RecoverFromFailure(request) => {
                signal_version_handover::Reply::RecoveryCompleted(
                    signal_version_handover::RecoveryResult {
                        component: request.component,
                        recovered: false,
                    },
                )
            }
        };
        trace.record(TraceNode::SPIRIT_ROOT, TraceAction::MessageReplied);
        Ok(RootUpgradeReply::new(reply, trace))
    }

    fn handover_rejected(
        component: version_projection::ComponentName,
        reason: signal_version_handover::HandoverRejectionReason,
    ) -> signal_version_handover::Reply {
        signal_version_handover::Reply::HandoverRejected(
            signal_version_handover::HandoverRejection { component, reason },
        )
    }

    async fn encode(&self, pipeline: PipelineReply) -> Result<TextReply> {
        let (reply, trace) = pipeline.into_parts();
        self.encoder
            .ask(reply::EncodeReply { reply, trace })
            .await
            .map_err(Self::text_send_error)
    }

    fn pipeline_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }

    fn frame_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }

    fn text_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }

    fn owner_send_error<Message>(error: SendError<Message, kameo::error::Infallible>) -> Error {
        Error::actor_runtime(error.to_string())
    }
}

impl RootTextReply {
    fn new(text: String, trace: ActorTrace) -> Self {
        Self { text, trace }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_text(self) -> String {
        self.text
    }
}

impl RootOperationReply {
    fn new(reply: signal_persona_spirit::Reply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &signal_persona_spirit::Reply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> signal_persona_spirit::Reply {
        self.reply
    }
}

impl RootFrameReply {
    fn new(reply: signal_frame::Reply<signal_persona_spirit::Reply>, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &signal_frame::Reply<signal_persona_spirit::Reply> {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> signal_frame::Reply<signal_persona_spirit::Reply> {
        self.reply
    }
}

impl RootOwnerReply {
    fn new(reply: owner_signal_persona_spirit::Reply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &owner_signal_persona_spirit::Reply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> owner_signal_persona_spirit::Reply {
        self.reply
    }
}

impl RootUpgradeReply {
    fn new(reply: signal_version_handover::Reply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &signal_version_handover::Reply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> signal_version_handover::Reply {
        self.reply
    }
}

impl SpiritActorRuntime {
    pub async fn start(store: StoreLocation) -> Result<Self> {
        Self::start_with_arguments(Arguments::new(store)).await
    }

    pub async fn start_with_bootstrap_policy_source(
        store: StoreLocation,
        bootstrap_policy_source: policy::BootstrapPolicySource,
    ) -> Result<Self> {
        Self::start_with_arguments(Arguments::with_bootstrap_policy_source(
            store,
            bootstrap_policy_source,
        ))
        .await
    }

    async fn start_with_arguments(arguments: Arguments) -> Result<Self> {
        Ok(Self {
            root: SpiritRoot::start(arguments).await?,
        })
    }

    pub async fn submit_text(&self, text: impl Into<String>) -> Result<RootTextReply> {
        self.root
            .ask(SubmitText { text: text.into() })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn submit_request(
        &self,
        request: signal_persona_spirit::Operation,
    ) -> Result<RootOperationReply> {
        self.root
            .ask(SubmitRequest { request })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn submit_frame_request(
        &self,
        request: signal_frame::Request<signal_persona_spirit::Operation>,
    ) -> Result<RootFrameReply> {
        self.root
            .ask(SubmitFrameRequest { request })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn submit_owner_request(
        &self,
        request: owner_signal_persona_spirit::Operation,
    ) -> Result<RootOwnerReply> {
        self.root
            .ask(SubmitOwnerRequest { request })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn submit_upgrade_request(
        &self,
        request: signal_version_handover::Operation,
    ) -> Result<RootUpgradeReply> {
        self.root
            .ask(SubmitUpgradeRequest { request })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn stop(self) -> Result<()> {
        SpiritRoot::stop(self.root).await
    }

    pub fn submit_text_blocking(
        store: StoreLocation,
        text: impl Into<String>,
    ) -> Result<RootTextReply> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|error| Error::actor_runtime(error.to_string()))?;
        runtime.block_on(async {
            let spirit = Self::start(store).await?;
            let reply = spirit.submit_text(text).await;
            let stop = spirit.stop().await;
            match (reply, stop) {
                (Ok(reply), Ok(())) => Ok(reply),
                (Err(error), _) => Err(error),
                (Ok(_), Err(error)) => Err(error),
            }
        })
    }

    fn root_send_error<Message>(error: SendError<Message, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            other => Error::actor_runtime(other.to_string()),
        }
    }
}

impl Actor for SpiritRoot {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        let store = store::RecordStore::supervise(
            &actor_reference,
            store::Arguments {
                location: arguments.store,
            },
        )
        .spawn_in_thread()
        .await;
        let policy = policy::PolicyPlane::supervise(
            &actor_reference,
            policy::Arguments {
                source: arguments.bootstrap_policy_source,
            },
        )
        .spawn()
        .await;
        let owner = owner::OwnerPlane::supervise(
            &actor_reference,
            owner::Arguments {
                lifecycle: owner::LifecycleState::default(),
                policy,
            },
        )
        .spawn()
        .await;
        let shaper =
            reply::ReplyShaper::supervise(&actor_reference, reply::ShaperArguments::default())
                .spawn()
                .await;
        let state = state::StatePlane::supervise(&actor_reference, state::Arguments::default())
            .spawn()
            .await;
        let classifier = classifier::ClassifierPlane::supervise(
            &actor_reference,
            classifier::Arguments::default(),
        )
        .spawn()
        .await;
        let clock = clock::ClockPlane::supervise(&actor_reference, clock::Arguments::default())
            .spawn()
            .await;
        let subscription = subscription::SubscriptionPlane::supervise(
            &actor_reference,
            subscription::Arguments::default(),
        )
        .spawn()
        .await;
        let dispatch = dispatch::DispatchPhase::supervise(
            &actor_reference,
            dispatch::Arguments {
                classifier,
                clock,
                store: store.clone(),
                state,
                subscription,
                reply: shaper,
            },
        )
        .spawn()
        .await;
        let decoder =
            decoder::NotaDecoder::supervise(&actor_reference, decoder::Arguments::default())
                .spawn()
                .await;
        let ingress = ingress::IngressPhase::supervise(
            &actor_reference,
            ingress::Arguments {
                decoder,
                dispatch: dispatch.clone(),
            },
        )
        .spawn()
        .await;
        let encoder = reply::ReplyTextEncoder::supervise(
            &actor_reference,
            reply::EncoderArguments::default(),
        )
        .spawn()
        .await;

        Ok(Self::new(owner, ingress, dispatch, encoder, store))
    }
}

impl Message<SubmitText> for SpiritRoot {
    type Reply = Result<RootTextReply>;

    async fn handle(
        &mut self,
        message: SubmitText,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.submit_text(message.text).await
    }
}

impl Message<SubmitRequest> for SpiritRoot {
    type Reply = Result<RootOperationReply>;

    async fn handle(
        &mut self,
        message: SubmitRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.submit_request(message.request).await
    }
}

impl Message<SubmitFrameRequest> for SpiritRoot {
    type Reply = Result<RootFrameReply>;

    async fn handle(
        &mut self,
        message: SubmitFrameRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.submit_frame_request(message.request).await
    }
}

impl Message<SubmitOwnerRequest> for SpiritRoot {
    type Reply = Result<RootOwnerReply>;

    async fn handle(
        &mut self,
        message: SubmitOwnerRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.submit_owner_request(message.request).await
    }
}

impl Message<SubmitUpgradeRequest> for SpiritRoot {
    type Reply = Result<RootUpgradeReply>;

    async fn handle(
        &mut self,
        message: SubmitUpgradeRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.submit_upgrade_request(message.request).await
    }
}
