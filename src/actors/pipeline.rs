use signal_frame::{Reply, SubReply};
use signal_persona_spirit::{SpiritReply, SpiritRequest};

use super::trace::ActorTrace;
use crate::{Error, Result};

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

#[derive(Debug, kameo::Reply)]
pub struct FramePipelineReply {
    reply: Reply<SpiritReply>,
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

impl FramePipelineReply {
    pub fn new(reply: Reply<SpiritReply>, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &Reply<SpiritReply> {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_parts(self) -> (Reply<SpiritReply>, ActorTrace) {
        (self.reply, self.trace)
    }

    pub fn into_reply(self) -> Reply<SpiritReply> {
        self.reply
    }

    pub fn into_single_pipeline_reply(self) -> Result<PipelineReply> {
        let (reply, trace) = self.into_parts();
        match reply {
            Reply::Accepted { per_operation, .. } if per_operation.len() == 1 => {
                match per_operation.into_head() {
                    SubReply::Ok(reply) => Ok(PipelineReply::new(reply, trace)),
                    SubReply::Failed {
                        detail: Some(reply),
                        ..
                    } => Ok(PipelineReply::new(reply, trace)),
                    other => Err(Error::UnexpectedFrame {
                        expected: "single accepted persona-spirit operation reply",
                        got: format!("{other:?}"),
                    }),
                }
            }
            Reply::Accepted { per_operation, .. } => Err(Error::UnexpectedFrame {
                expected: "one persona-spirit operation reply",
                got: format!("{} operation replies", per_operation.len()),
            }),
            Reply::Rejected { reason } => Err(Error::RequestRejected {
                reason: reason.to_string(),
            }),
        }
    }
}
