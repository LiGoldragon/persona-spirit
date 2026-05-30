use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use nota_codec::{Encoder, NotaEncode};
use signal_persona_spirit::{
    OperationKind, Reply as WorkingReply, RequestUnimplemented, UnimplementedReason,
};

use crate::{Error, Result};

use super::pipeline::PipelineReply;
use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct ReplyShaper {
    policy: UnimplementedPolicy,
}

pub struct ReplyTextEncoder {
    policy: EncodingPolicy,
}

#[derive(Clone, Default)]
pub struct ShaperArguments {
    pub policy: UnimplementedPolicy,
}

#[derive(Clone, Default)]
pub struct EncoderArguments {
    pub policy: EncodingPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnimplementedPolicy {
    not_built: UnimplementedReason,
    integration_missing: UnimplementedReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodingPolicy {
    canonical: bool,
}

pub struct ShapeUnimplemented {
    pub operation: OperationKind,
    pub trace: ActorTrace,
}

pub struct EncodeReply {
    pub reply: WorkingReply,
    pub trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct TextReply {
    text: String,
    reply: WorkingReply,
    trace: ActorTrace,
}

impl Default for UnimplementedPolicy {
    fn default() -> Self {
        Self {
            not_built: UnimplementedReason::NotBuiltYet,
            integration_missing: UnimplementedReason::IntegrationNotLanded,
        }
    }
}

impl Default for EncodingPolicy {
    fn default() -> Self {
        Self { canonical: true }
    }
}

impl ReplyShaper {
    fn new(policy: UnimplementedPolicy) -> Self {
        Self { policy }
    }

    fn shape_unimplemented(
        &self,
        operation: OperationKind,
        mut trace: ActorTrace,
    ) -> PipelineReply {
        trace.record(TraceNode::REPLY_SHAPER, TraceAction::MessageReceived);
        let reply = WorkingReply::RequestUnimplemented(RequestUnimplemented {
            reason: self.policy.reason_for(operation),
        });
        trace.record(TraceNode::REPLY_SHAPER, TraceAction::MessageReplied);
        PipelineReply::new(reply, trace)
    }
}

impl ReplyTextEncoder {
    fn new(policy: EncodingPolicy) -> Self {
        Self { policy }
    }

    fn encode_reply(&self, reply: WorkingReply, mut trace: ActorTrace) -> Result<TextReply> {
        trace.record(TraceNode::REPLY_TEXT_ENCODER, TraceAction::MessageReceived);
        let mut encoder = Encoder::new();
        reply
            .encode(&mut encoder)
            .map_err(Error::invalid_spirit_reply)?;
        let text = encoder.into_string();
        let _canonical = self.policy.canonical;
        trace.record(TraceNode::REPLY_TEXT_ENCODER, TraceAction::TextEncoded);
        Ok(TextReply::new(text, reply, trace))
    }
}

impl UnimplementedPolicy {
    pub fn reason_for(self, operation: OperationKind) -> UnimplementedReason {
        match operation {
            OperationKind::State
            | OperationKind::Observe
            | OperationKind::Watch
            | OperationKind::Unwatch
            | OperationKind::Remove
            | OperationKind::ChangeCertainty
            | OperationKind::Tap
            | OperationKind::Untap => self.not_built,
            OperationKind::Record => self.integration_missing,
        }
    }
}

impl TextReply {
    pub fn new(text: String, reply: WorkingReply, trace: ActorTrace) -> Self {
        Self { text, reply, trace }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    pub fn reply(&self) -> &WorkingReply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_text(self) -> String {
        self.text
    }
}

impl Actor for ReplyShaper {
    type Args = ShaperArguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.policy))
    }
}

impl Actor for ReplyTextEncoder {
    type Args = EncoderArguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.policy))
    }
}

impl Message<ShapeUnimplemented> for ReplyShaper {
    type Reply = PipelineReply;

    async fn handle(
        &mut self,
        message: ShapeUnimplemented,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.shape_unimplemented(message.operation, message.trace)
    }
}

impl Message<EncodeReply> for ReplyTextEncoder {
    type Reply = Result<TextReply>;

    async fn handle(
        &mut self,
        message: EncodeReply,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.encode_reply(message.reply, message.trace)
    }
}
