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

pub struct SpiritRoot {
    owner: ActorRef<owner::OwnerPlane>,
    ingress: ActorRef<ingress::IngressPhase>,
    dispatch: ActorRef<dispatch::DispatchPhase>,
    encoder: ActorRef<reply::ReplyTextEncoder>,
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
    pub request: signal_persona_spirit::SpiritRequest,
}

pub struct SubmitFrameRequest {
    pub request: signal_frame::Request<signal_persona_spirit::SpiritRequest>,
}

pub struct SubmitOwnerRequest {
    pub request: owner_signal_persona_spirit::OwnerSpiritRequest,
}

#[derive(Debug, kameo::Reply)]
pub struct RootTextReply {
    text: String,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootOperationReply {
    reply: signal_persona_spirit::SpiritReply,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootFrameReply {
    reply: signal_frame::Reply<signal_persona_spirit::SpiritReply>,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct RootOwnerReply {
    reply: owner_signal_persona_spirit::OwnerSpiritReply,
    trace: ActorTrace,
}

pub struct SpiritActorRuntime {
    root: ActorRef<SpiritRoot>,
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
    ) -> Self {
        Self {
            owner,
            ingress,
            dispatch,
            encoder,
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
        request: signal_persona_spirit::SpiritRequest,
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
        request: signal_frame::Request<signal_persona_spirit::SpiritRequest>,
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
        request: owner_signal_persona_spirit::OwnerSpiritRequest,
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
    fn new(reply: signal_persona_spirit::SpiritReply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &signal_persona_spirit::SpiritReply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> signal_persona_spirit::SpiritReply {
        self.reply
    }
}

impl RootFrameReply {
    fn new(
        reply: signal_frame::Reply<signal_persona_spirit::SpiritReply>,
        trace: ActorTrace,
    ) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &signal_frame::Reply<signal_persona_spirit::SpiritReply> {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> signal_frame::Reply<signal_persona_spirit::SpiritReply> {
        self.reply
    }
}

impl RootOwnerReply {
    fn new(reply: owner_signal_persona_spirit::OwnerSpiritReply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &owner_signal_persona_spirit::OwnerSpiritReply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_reply(self) -> owner_signal_persona_spirit::OwnerSpiritReply {
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
        request: signal_persona_spirit::SpiritRequest,
    ) -> Result<RootOperationReply> {
        self.root
            .ask(SubmitRequest { request })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn submit_frame_request(
        &self,
        request: signal_frame::Request<signal_persona_spirit::SpiritRequest>,
    ) -> Result<RootFrameReply> {
        self.root
            .ask(SubmitFrameRequest { request })
            .await
            .map_err(Self::root_send_error)
    }

    pub async fn submit_owner_request(
        &self,
        request: owner_signal_persona_spirit::OwnerSpiritRequest,
    ) -> Result<RootOwnerReply> {
        self.root
            .ask(SubmitOwnerRequest { request })
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
                store,
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

        Ok(Self::new(owner, ingress, dispatch, encoder))
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
