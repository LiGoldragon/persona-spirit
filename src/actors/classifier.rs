use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context as ActorContext, Message};
use signal_persona_spirit::{
    Certainty, Context, Date, Entry, Kind, Quote, Statement, Summary, Time, Topic,
};

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
    fallback_certainty: Certainty,
    fallback_context: Context,
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
            fallback_certainty: Certainty::Minimum,
            fallback_context: Context::new(
                "captured from State operation by provisional classifier",
            ),
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
        let clock = self.policy.clock_reading_now();
        let entry = Entry {
            topic: self.policy.fallback_topic.clone(),
            kind: self.policy.fallback_kind,
            summary: Summary::new(text.clone()),
            context: self.policy.fallback_context.clone(),
            certainty: self.policy.fallback_certainty,
            date: clock.date,
            time: clock.time,
            quote: Quote::new(text),
        };
        trace.record(
            TraceNode::CLASSIFIER_PLANE,
            TraceAction::StatementClassified,
        );
        ClassifiedEntry { entry, trace }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClockReading {
    date: Date,
    time: Time,
}

impl ClassificationPolicy {
    fn clock_reading_now(&self) -> ClockReading {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        ClockReading::from_unix_seconds(seconds)
    }
}

impl ClockReading {
    fn from_unix_seconds(seconds: u64) -> Self {
        let days = (seconds / 86_400) as i64;
        let seconds_of_day = seconds % 86_400;
        let (year, month, day) = civil_date_from_unix_days(days);
        let hour = (seconds_of_day / 3_600) as u8;
        let minute = ((seconds_of_day % 3_600) / 60) as u8;
        let second = (seconds_of_day % 60) as u8;
        Self {
            date: Date::new(year as u16, month as u8, day as u8),
            time: Time::new(hour, minute, second),
        }
    }
}

fn civil_date_from_unix_days(days: i64) -> (i32, u32, u32) {
    let zero_based_days = days + 719_468;
    let era = if zero_based_days >= 0 {
        zero_based_days
    } else {
        zero_based_days - 146_096
    } / 146_097;
    let day_of_era = zero_based_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_parameter = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_parameter + 2) / 5 + 1;
    let month = month_parameter + if month_parameter < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year as i32, month as u32, day as u32)
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
