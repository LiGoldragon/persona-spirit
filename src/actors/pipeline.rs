use signal_persona_spirit::{SpiritReply, SpiritRequest};

use super::trace::ActorTrace;

#[derive(Debug, kameo::Reply)]
pub struct DecodedRequest {
    request: SpiritRequest,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct PipelineReply {
    reply: SpiritReply,
    trace: ActorTrace,
}

impl DecodedRequest {
    pub fn new(request: SpiritRequest, trace: ActorTrace) -> Self {
        Self { request, trace }
    }

    pub fn into_parts(self) -> (SpiritRequest, ActorTrace) {
        (self.request, self.trace)
    }
}

impl PipelineReply {
    pub fn new(reply: SpiritReply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &SpiritReply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_parts(self) -> (SpiritReply, ActorTrace) {
        (self.reply, self.trace)
    }
}
