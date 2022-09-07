use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use rand::Rng;

use crate::{
    frame::{FrameOpcode, RequestOpcode, ResponseOpcode},
    RequestFrame, ResponseFrame,
};

/// Specifies when an associated `Reaction` will be performed.
/// Conditions are subject to logic, with `not()`, `and()` and `or()`
/// convenience functions.
#[derive(Debug, Clone)]
pub enum Condition {
    True,

    False,

    Not(Box<Condition>),

    And(Box<Condition>, Box<Condition>),

    Or(Box<Condition>, Box<Condition>),

    /// True iff the frame has come in the n-th driver connection established with the driver.
    ConnectionSeqNo(usize),

    /// True iff the frame has the given opcode (and is a request).
    RequestOpcode(RequestOpcode),

    /// True iff the frame has the given opcode (and is a response).
    ResponseOpcode(ResponseOpcode),

    /// True iff the frame body contains the given byte slice, with case-sensitive comparison.
    BodyContainsCaseSensitive(Box<[u8]>),

    /// True iff the frame body contains the given byte slice, with case-insensitive comparison (ASCII only).
    BodyContainsCaseInsensitive(Box<[u8]>),

    /// True with the given probability.
    RandomWithProbability(f64),
}

/// The context in which [`Conditions`](Condition) are evaluated.
pub(crate) struct EvaluationContext {
    pub(crate) connection_seq_no: usize,
    pub(crate) opcode: FrameOpcode,
    pub(crate) frame_body: Bytes,
}

impl Condition {
    pub(crate) fn eval(&self, ctx: &EvaluationContext) -> bool {
        match self {
            Condition::True => true,

            Condition::False => false,

            Condition::Not(c) => !c.eval(ctx),

            Condition::And(c1, c2) => c1.eval(ctx) && c2.eval(ctx),

            Condition::Or(c1, c2) => c1.eval(ctx) || c2.eval(ctx),

            Condition::ConnectionSeqNo(no) => *no == ctx.connection_seq_no,

            Condition::RequestOpcode(op1) => match ctx.opcode {
                FrameOpcode::Request(op2) => *op1 == op2,
                FrameOpcode::Response(_) => panic!(
                    "Invalid type applied in rule condition: driver request opcode in cluster context"
                ),
            },

            Condition::ResponseOpcode(op1) => match ctx.opcode {
                FrameOpcode::Request(_) => panic!(
                    "Invalid type applied in rule condition: cluster response opcode in driver context"
                ),
                FrameOpcode::Response(op2) => *op1 == op2,
            },

            Condition::BodyContainsCaseSensitive(pattern) => ctx
                .frame_body
                .windows(pattern.len())
                .any(|window| *window == **pattern),

            Condition::BodyContainsCaseInsensitive(pattern) => std::str::from_utf8(pattern)
                .map(|pattern_str| {
                    ctx.frame_body.windows(pattern.len()).any(|window| {
                        std::str::from_utf8(window)
                            .map(|window_str| str::eq_ignore_ascii_case(window_str, pattern_str))
                            .unwrap_or(false)
                    })
                })
                .unwrap_or(false),
            Condition::RandomWithProbability(probability) => {
                rand::thread_rng().gen_bool(*probability)
            }
        }
    }

    /// A convenience function for creating [Condition::Not] variant.
    #[allow(clippy::should_implement_trait)]
    pub fn not(c: Self) -> Self {
        Condition::Not(Box::new(c))
    }

    /// A convenience function for creating [Condition::And] variant.
    pub fn and(self, c2: Self) -> Self {
        Self::And(Box::new(self), Box::new(c2))
    }

    /// A convenience function for creating [Condition::Or] variant.
    pub fn or(self, c2: Self) -> Self {
        Self::Or(Box::new(self), Box::new(c2))
    }
}

#[derive(Clone)]
pub struct RequestReaction {
    pub to_addressee: Option<Action<RequestFrame, RequestFrame>>,
    pub to_sender: Option<Action<RequestFrame, ResponseFrame>>,
    pub drop_connection: Option<Option<Duration>>,
}

#[derive(Clone)]
pub struct ResponseReaction {
    pub to_addressee: Option<Action<ResponseFrame, ResponseFrame>>,
    pub to_sender: Option<Action<ResponseFrame, RequestFrame>>,
    pub drop_connection: Option<Option<Duration>>,
}

/// Describes what to with the given <something> (frame),
/// how to transform it and after what delay.
#[derive(Clone)]
pub struct Action<TFrom, TTo> {
    pub delay: Option<Duration>,
    pub msg_processor: Option<Arc<dyn Fn(TFrom) -> TTo + Send + Sync>>,
}

/// A rule describing what actions should the proxy perform
/// with the received request frame and on what conditions.
#[derive(Clone)]
pub struct RequestRule(pub Condition, pub RequestReaction);

/// A rule describing what actions should the proxy perform
/// with the received response frame and on what conditions.
#[derive(Clone)]
pub struct ResponseRule(pub Condition, pub ResponseReaction);

#[test]
fn condition_case_insensitive_matching() {
    let condition_matching =
        Condition::BodyContainsCaseInsensitive(Box::new(*b"cassandra'sInefficiency"));
    let condition_nonmatching =
        Condition::BodyContainsCaseInsensitive(Box::new(*b"cassandrasInefficiency"));
    let ctx = EvaluationContext {
        connection_seq_no: 42,
        opcode: FrameOpcode::Request(RequestOpcode::Options),
        frame_body: Bytes::from_static(b"\0\0x{0x223}Cassandra'sINEFFICIENCY\x12\x31"),
    };

    assert!(condition_matching.eval(&ctx));
    assert!(!condition_nonmatching.eval(&ctx));
}
