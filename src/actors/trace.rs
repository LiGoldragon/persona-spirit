#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceNode {
    label: &'static str,
}

impl TraceNode {
    pub const SPIRIT_ROOT: Self = Self::new("SpiritRoot");
    pub const INGRESS_PHASE: Self = Self::new("IngressPhase");
    pub const NOTA_DECODER: Self = Self::new("NotaDecoder");
    pub const CLASSIFIER_PLANE: Self = Self::new("ClassifierPlane");
    pub const CLOCK_PLANE: Self = Self::new("ClockPlane");
    pub const DISPATCH_PHASE: Self = Self::new("DispatchPhase");
    pub const OWNER_PLANE: Self = Self::new("OwnerPlane");
    pub const POLICY_PLANE: Self = Self::new("PolicyPlane");
    pub const STATE_PLANE: Self = Self::new("StatePlane");
    pub const SUBSCRIPTION_PLANE: Self = Self::new("SubscriptionPlane");
    pub const RECORD_STORE: Self = Self::new("RecordStore");
    pub const SIGNAL_EXECUTOR: Self = Self::new("SignalExecutor");
    pub const SEMA_OBSERVER: Self = Self::new("SemaObserver");
    pub const SEMA_WRITER: Self = Self::new("SemaWriter");
    pub const SEMA_READER: Self = Self::new("SemaReader");
    pub const REPLY_SHAPER: Self = Self::new("ReplyShaper");
    pub const REPLY_TEXT_ENCODER: Self = Self::new("ReplyTextEncoder");

    pub const fn new(label: &'static str) -> Self {
        Self { label }
    }

    pub fn label(self) -> &'static str {
        self.label
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TraceAction {
    ActorStarted,
    MessageReceived,
    RequestDecoded,
    OperationReceived,
    ObservationProjected,
    StatementClassified,
    EntryStamped,
    RecordCommitted,
    RecordsRead,
    SubscriptionOpened,
    SubscriptionRetracted,
    TextEncoded,
    MessageReplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceEvent {
    node: TraceNode,
    action: TraceAction,
}

impl TraceEvent {
    pub fn new(node: TraceNode, action: TraceAction) -> Self {
        Self { node, action }
    }

    pub fn node(&self) -> TraceNode {
        self.node
    }

    pub fn action(&self) -> TraceAction {
        self.action
    }
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct ActorTrace {
    events: Vec<TraceEvent>,
}

impl ActorTrace {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    pub fn record(&mut self, node: TraceNode, action: TraceAction) {
        self.events.push(TraceEvent::new(node, action));
    }

    pub fn contains(&self, node: TraceNode) -> bool {
        self.events.iter().any(|event| event.node == node)
    }

    pub fn contains_action(&self, node: TraceNode, action: TraceAction) -> bool {
        self.events()
            .iter()
            .any(|event| event.node == node && event.action == action)
    }

    pub fn contains_ordered(&self, nodes: &[TraceNode]) -> bool {
        let mut remaining = nodes.iter();
        let Some(mut expected) = remaining.next() else {
            return true;
        };

        for event in self.events() {
            if event.node == *expected {
                match remaining.next() {
                    Some(next) => expected = next,
                    None => return true,
                }
            }
        }

        false
    }
}

impl Default for ActorTrace {
    fn default() -> Self {
        Self::new()
    }
}
