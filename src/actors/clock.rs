use std::time::{SystemTime, UNIX_EPOCH};

use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_persona_spirit::{Date, Entry, Time};

use crate::store::StampedEntry;

use super::trace::{ActorTrace, TraceAction, TraceNode};

pub struct ClockPlane {
    source: ClockSource,
}

#[derive(Clone, Default)]
pub struct Arguments {
    pub source: ClockSource,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct StampedEntryReply {
    pub entry: StampedEntry,
    pub trace: ActorTrace,
}

pub struct StampEntry {
    pub entry: Entry,
    pub trace: ActorTrace,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClockSource {
    offset_seconds: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClockReading {
    date: Date,
    time: Time,
}

impl ClockPlane {
    fn new(source: ClockSource) -> Self {
        Self { source }
    }

    fn stamp_entry(&self, entry: Entry, mut trace: ActorTrace) -> StampedEntryReply {
        trace.record(TraceNode::CLOCK_PLANE, TraceAction::MessageReceived);
        let reading = self.source.read();
        let entry = StampedEntry::new(entry, reading.date, reading.time);
        trace.record(TraceNode::CLOCK_PLANE, TraceAction::EntryStamped);
        StampedEntryReply { entry, trace }
    }
}

impl ClockSource {
    fn read(self) -> ClockReading {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0)
            + self.offset_seconds;
        ClockReading::from_unix_seconds(seconds.max(0) as u64)
    }
}

impl ClockReading {
    fn from_unix_seconds(seconds: u64) -> Self {
        let days = (seconds / 86_400) as i64;
        let seconds_of_day = seconds % 86_400;
        let (year, month, day) = CivilDate::from_unix_days(days).into_parts();
        let hour = (seconds_of_day / 3_600) as u8;
        let minute = ((seconds_of_day % 3_600) / 60) as u8;
        let second = (seconds_of_day % 60) as u8;
        Self {
            date: Date::new(year as u16, month as u8, day as u8),
            time: Time::new(hour, minute, second),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CivilDate {
    year: i32,
    month: u32,
    day: u32,
}

impl CivilDate {
    fn from_unix_days(days: i64) -> Self {
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
        Self {
            year: year as i32,
            month: month as u32,
            day: day as u32,
        }
    }

    fn into_parts(self) -> (i32, u32, u32) {
        (self.year, self.month, self.day)
    }
}

impl Actor for ClockPlane {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(Self::new(arguments.source))
    }
}

impl Message<StampEntry> for ClockPlane {
    type Reply = StampedEntryReply;

    async fn handle(
        &mut self,
        message: StampEntry,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.stamp_entry(message.entry, message.trace)
    }
}
