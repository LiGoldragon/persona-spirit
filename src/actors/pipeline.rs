use signal_frame::{Reply, SubReply};
use signal_persona_spirit::{Operation as WorkingOperation, Reply as WorkingReply};

use super::trace::ActorTrace;
use crate::{Error, Result};

#[derive(Debug, kameo::Reply)]
pub struct DecodedRequest {
    request: WorkingOperation,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct PipelineReply {
    reply: WorkingReply,
    trace: ActorTrace,
}

#[derive(Debug, kameo::Reply)]
pub struct FramePipelineReply {
    reply: Reply<WorkingReply>,
    trace: ActorTrace,
}

impl DecodedRequest {
    pub fn new(request: WorkingOperation, trace: ActorTrace) -> Self {
        Self { request, trace }
    }

    pub fn into_parts(self) -> (WorkingOperation, ActorTrace) {
        (self.request, self.trace)
    }
}

impl PipelineReply {
    pub fn new(reply: WorkingReply, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &WorkingReply {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_parts(self) -> (WorkingReply, ActorTrace) {
        (self.reply, self.trace)
    }
}

impl FramePipelineReply {
    pub fn new(reply: Reply<WorkingReply>, trace: ActorTrace) -> Self {
        Self { reply, trace }
    }

    pub fn reply(&self) -> &Reply<WorkingReply> {
        &self.reply
    }

    pub fn trace(&self) -> &ActorTrace {
        &self.trace
    }

    pub fn into_parts(self) -> (Reply<WorkingReply>, ActorTrace) {
        (self.reply, self.trace)
    }

    pub fn into_reply(self) -> Reply<WorkingReply> {
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
