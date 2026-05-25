use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context as ActorContext, Message};
use signal_persona_spirit::{Description, Entry, Kind, Statement, Topic};
use signal_sema::Magnitude;

use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct ClassifierPlane {
    policy: ClassificationPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct ClassifiedEntry {
    pub entry: Entry,
    pub trace: ActorTrace,
}

#[derive(Clone, Default)]
pub struct Arguments {
    pub policy: ClassificationPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassificationPolicy {
    fallback_topic: Topic,
    fallback_kind: Kind,
    fallback_certainty: Magnitude,
}

pub struct ClassifyStatement {
    pub statement: Statement,
    pub trace: ActorTrace,
}

impl Default for ClassificationPolicy {
    fn default() -> Self {
        Self {
            fallback_topic: Topic::new("unclassified"),
            fallback_kind: Kind::Clarification,
            fallback_certainty: Magnitude::Minimum,
        }
    }
}

impl ClassifierPlane {
    fn new(policy: ClassificationPolicy) -> Self {
        Self { policy }
    }

    fn classify(&self, statement: Statement, mut trace: ActorTrace) -> ClassifiedEntry {
        trace.record(TraceNode::CLASSIFIER_PLANE, TraceAction::MessageReceived);
        let text = statement.text.as_str().to_string();
        let entry = Entry {
            topic: self.policy.fallback_topic.clone(),
            kind: self.policy.fallback_kind,
            description: Description::new(text.clone()),
            certainty: self.policy.fallback_certainty,
        };
        trace.record(
            TraceNode::CLASSIFIER_PLANE,
            TraceAction::StatementClassified,
        );
        ClassifiedEntry { entry, trace }
    }
}

impl Actor for ClassifierPlane {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.policy))
    }
}

impl Message<ClassifyStatement> for ClassifierPlane {
    type Reply = ClassifiedEntry;

    async fn handle(
        &mut self,
        message: ClassifyStatement,
        _context: &mut ActorContext<Self, Self::Reply>,
    ) -> Self::Reply {
        self.classify(message.statement, message.trace)
    }
}
