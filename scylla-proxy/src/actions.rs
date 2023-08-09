use std::{fmt, sync::Arc, time::Duration};

use bytes::Bytes;
use rand::{Rng, RngCore};
use tokio::sync::mpsc;

use crate::{
    frame::{FrameOpcode, FrameParams, RequestFrame, RequestOpcode, ResponseFrame, ResponseOpcode},
    TargetShard,
};
use scylla_cql::{
    errors::{DbError, WriteType},
    Consistency,
};

/// Specifies when an associated [Reaction] will be performed.
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

    /// True for predefined number of evaluations, then always false.
    TrueForLimitedTimes(usize),
}

/// The context in which [`Conditions`](Condition) are evaluated.
pub(crate) struct EvaluationContext {
    pub(crate) connection_seq_no: usize,
    pub(crate) opcode: FrameOpcode,
    pub(crate) frame_body: Bytes,
}

impl Condition {
    pub(crate) fn eval(&mut self, ctx: &EvaluationContext) -> bool {
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

            Condition::TrueForLimitedTimes(times) => {
                let val = *times > 0;
                if val {
                    *times -= 1;
                }
                val
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

/// Just a trait to unify API of both [RequestReaction] and [ResponseReaction].
/// As they are both analogous, I will describe them here.
/// - `to_addressee` and `to_sender` field correspond to actions that the proxy should perform
///   towards the frame's intended receiver and sender, respectively.
/// - `drop_connection`'s outer `Option` denotes whether proxy should drop connection after
///   performing the remaining actions, and its inner `Option` contains the delay of the drop.
/// - `feedback_channel` is a channel to which proxy shall send any frame that matches the rule.
///   It can be useful for testing that a particular node was the intended adressee of the frame.
///
/// `Reaction` contains useful constructors of common-case Reactions. The names should be
/// self-explanatory.
pub trait Reaction: Sized {
    type Incoming;
    type Returning;

    /// Does nothing extraordinary, i.e. passes the frame with no changes to it.
    fn noop() -> Self;

    /// Drops frame, i.e. passes it into void.
    fn drop_frame() -> Self;

    /// Passes the frame only after specified delay.
    fn delay(time: Duration) -> Self;

    /// Instead of passing the frame to the addressee, returns the forged frame back to the addresser.
    fn forge_response(f: Arc<dyn Fn(Self::Incoming) -> Self::Returning + Send + Sync>) -> Self;

    /// The same as [forge_response](Self::forge_response), but with specified delay.
    fn forge_response_with_delay(
        time: Duration,
        f: Arc<dyn Fn(Self::Incoming) -> Self::Returning + Send + Sync>,
    ) -> Self;

    /// Drops the frame AND drops the connection with both the driver and the cluster.
    fn drop_connection() -> Self;

    /// The same as [drop_connection](Self::drop_connection), but with specified delay.
    fn drop_connection_with_delay(time: Duration) -> Self;

    /// Adds sending the matching frame as feedback using the provided channel.
    /// Modifies the existing `Reaction`.
    fn with_feedback_when_performed(
        self,
        tx: mpsc::UnboundedSender<(Self::Incoming, Option<TargetShard>)>,
    ) -> Self;
}

fn fmt_reaction(
    f: &mut std::fmt::Formatter<'_>,
    reaction_type: &str,
    to_addressee: &dyn fmt::Debug,
    to_sender: &dyn fmt::Debug,
    drop_connection: &dyn fmt::Debug,
    has_feedback_channel: bool,
) -> std::fmt::Result {
    f.debug_struct(reaction_type)
        .field("to_addressee", to_addressee)
        .field("to_sender", to_sender)
        .field("drop_connection", drop_connection)
        .field(
            "feedback_channel",
            if has_feedback_channel {
                &"Some(<feedback_channel>)"
            } else {
                &"None"
            },
        )
        .finish()
}

#[derive(Clone)]
pub struct RequestReaction {
    pub to_addressee: Option<Action<RequestFrame, RequestFrame>>,
    pub to_sender: Option<Action<RequestFrame, ResponseFrame>>,
    pub drop_connection: Option<Option<Duration>>,
    pub feedback_channel: Option<mpsc::UnboundedSender<(RequestFrame, Option<TargetShard>)>>,
}

impl fmt::Debug for RequestReaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_reaction(
            f,
            "RequestReaction",
            &self.to_addressee,
            &self.to_sender,
            &self.drop_connection,
            self.feedback_channel.is_some(),
        )
    }
}

#[derive(Clone)]
pub struct ResponseReaction {
    pub to_addressee: Option<Action<ResponseFrame, ResponseFrame>>,
    pub to_sender: Option<Action<ResponseFrame, RequestFrame>>,
    pub drop_connection: Option<Option<Duration>>,
    pub feedback_channel: Option<mpsc::UnboundedSender<(ResponseFrame, Option<TargetShard>)>>,
}

impl fmt::Debug for ResponseReaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_reaction(
            f,
            "ResponseReaction",
            &self.to_addressee,
            &self.to_sender,
            &self.drop_connection,
            self.feedback_channel.is_some(),
        )
    }
}

impl Reaction for RequestReaction {
    type Incoming = RequestFrame;
    type Returning = ResponseFrame;

    fn noop() -> Self {
        RequestReaction {
            to_addressee: Some(Action {
                delay: None,
                msg_processor: None,
            }),
            to_sender: None,
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn drop_frame() -> Self {
        RequestReaction {
            to_addressee: None,
            to_sender: None,
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn delay(time: Duration) -> Self {
        RequestReaction {
            to_addressee: Some(Action {
                delay: Some(time),
                msg_processor: None,
            }),
            to_sender: None,
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn forge_response(f: Arc<dyn Fn(Self::Incoming) -> Self::Returning + Send + Sync>) -> Self {
        RequestReaction {
            to_addressee: None,
            to_sender: Some(Action {
                delay: None,
                msg_processor: Some(f),
            }),
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn forge_response_with_delay(
        time: Duration,
        f: Arc<dyn Fn(Self::Incoming) -> Self::Returning + Send + Sync>,
    ) -> Self {
        RequestReaction {
            to_addressee: None,
            to_sender: Some(Action {
                delay: Some(time),
                msg_processor: Some(f),
            }),
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn drop_connection() -> Self {
        RequestReaction {
            to_addressee: None,
            to_sender: None,
            drop_connection: Some(None),
            feedback_channel: None,
        }
    }

    fn drop_connection_with_delay(time: Duration) -> Self {
        RequestReaction {
            to_addressee: None,
            to_sender: None,
            drop_connection: Some(Some(time)),
            feedback_channel: None,
        }
    }

    fn with_feedback_when_performed(
        self,
        tx: mpsc::UnboundedSender<(Self::Incoming, Option<TargetShard>)>,
    ) -> Self {
        Self {
            feedback_channel: Some(tx),
            ..self
        }
    }
}

impl RequestReaction {
    pub fn forge_with_error_lazy(gen_error: Box<dyn Fn() -> DbError + Send + Sync>) -> Self {
        Self::forge_with_error_lazy_delay(gen_error, None)
    }
    /// A convenient shortcut for forging a various error-type responses, useful e.g. for testing retries.
    /// Errors are computed on-demand by the provided closure.
    pub fn forge_with_error_lazy_delay(
        gen_error: Box<dyn Fn() -> DbError + Send + Sync>,
        delay: Option<Duration>,
    ) -> Self {
        RequestReaction {
            to_addressee: None,
            to_sender: Some(Action {
                delay,
                msg_processor: Some(Arc::new(move |request: RequestFrame| {
                    ResponseFrame::forged_error(request.params.for_response(), gen_error(), None)
                        .unwrap()
                })),
            }),
            drop_connection: None,
            feedback_channel: None,
        }
    }

    pub fn forge_with_error(error: DbError) -> Self {
        Self::forge_with_error_and_message(error, Some("Proxy-triggered error.".into()))
    }

    /// A convenient shortcut for forging a various error-type responses, useful e.g. for testing retries.
    pub fn forge_with_error_and_message(error: DbError, msg: Option<String>) -> Self {
        // sanity create-time check
        ResponseFrame::forged_error(
            FrameParams {
                version: 0,
                flags: 0,
                stream: 0,
            },
            error.clone(),
            None,
        )
        .unwrap_or_else(|_| panic!("Invalid DbError provided: {:#?}", error));

        RequestReaction {
            to_addressee: None,
            to_sender: Some(Action {
                delay: None,
                msg_processor: Some(Arc::new(move |request: RequestFrame| {
                    ResponseFrame::forged_error(
                        request.params.for_response(),
                        error.clone(),
                        msg.as_deref(),
                    )
                    .unwrap()
                })),
            }),
            drop_connection: None,
            feedback_channel: None,
        }
    }

    pub fn forge() -> ResponseForger {
        ResponseForger
    }
}

struct ExampleDbErrors;
impl ExampleDbErrors {
    pub fn syntax_error() -> DbError {
        DbError::SyntaxError
    }
    pub fn invalid() -> DbError {
        DbError::Invalid
    }
    pub fn already_exists() -> DbError {
        DbError::AlreadyExists {
            keyspace: "proxy".into(),
            table: "worker".into(),
        }
    }
    pub fn function_failure() -> DbError {
        DbError::FunctionFailure {
            keyspace: "proxy".into(),
            function: "fibonacci".into(),
            arg_types: vec!["n".into()],
        }
    }
    pub fn authentication_error() -> DbError {
        DbError::AuthenticationError
    }
    pub fn unauthorized() -> DbError {
        DbError::Unauthorized
    }
    pub fn config_error() -> DbError {
        DbError::ConfigError
    }
    pub fn unavailable() -> DbError {
        DbError::Unavailable {
            consistency: Consistency::One,
            required: 2,
            alive: 1,
        }
    }
    pub fn overloaded() -> DbError {
        DbError::Overloaded
    }
    pub fn is_bootstrapping() -> DbError {
        DbError::IsBootstrapping
    }
    pub fn truncate_error() -> DbError {
        DbError::TruncateError
    }
    pub fn read_timeout() -> DbError {
        DbError::ReadTimeout {
            consistency: Consistency::One,
            received: 2,
            required: 3,
            data_present: true,
        }
    }
    pub fn write_timeout() -> DbError {
        DbError::WriteTimeout {
            consistency: Consistency::One,
            received: 2,
            required: 3,
            write_type: WriteType::UnloggedBatch,
        }
    }
    pub fn read_failure() -> DbError {
        DbError::ReadFailure {
            consistency: Consistency::One,
            received: 2,
            required: 3,
            data_present: true,
            numfailures: 1,
        }
    }
    pub fn write_failure() -> DbError {
        DbError::WriteFailure {
            consistency: Consistency::One,
            received: 2,
            required: 3,
            write_type: WriteType::UnloggedBatch,
            numfailures: 1,
        }
    }
    pub fn unprepared() -> DbError {
        DbError::Unprepared {
            statement_id: Bytes::from_static(b"21372137"),
        }
    }
    pub fn server_error() -> DbError {
        DbError::ServerError
    }
    pub fn protocol_error() -> DbError {
        DbError::ProtocolError
    }
    pub fn other(num: i32) -> DbError {
        DbError::Other(num)
    }
}

pub struct ResponseForger;

impl ResponseForger {
    pub fn syntax_error(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::syntax_error())
    }
    pub fn invalid(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::invalid())
    }
    pub fn already_exists(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::already_exists())
    }
    pub fn function_failure(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::function_failure())
    }
    pub fn authentication_error(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::authentication_error())
    }
    pub fn unauthorized(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::unauthorized())
    }
    pub fn config_error(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::config_error())
    }
    pub fn unavailable(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::unavailable())
    }
    pub fn overloaded(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::overloaded())
    }
    pub fn is_bootstrapping(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::is_bootstrapping())
    }
    pub fn truncate_error(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::truncate_error())
    }
    pub fn read_timeout(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::read_timeout())
    }
    pub fn write_timeout(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::write_timeout())
    }
    pub fn read_failure(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::read_failure())
    }
    pub fn write_failure(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::write_failure())
    }
    pub fn unprepared(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::unprepared())
    }
    pub fn server_error(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::server_error())
    }
    pub fn protocol_error(&self) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::protocol_error())
    }
    pub fn other(&self, num: i32) -> RequestReaction {
        RequestReaction::forge_with_error(ExampleDbErrors::other(num))
    }
    pub fn random_error(&self) -> RequestReaction {
        self.random_error_with_delay(None)
    }
    pub fn random_error_with_delay(&self, delay: Option<Duration>) -> RequestReaction {
        static ERRORS: &[fn() -> DbError] = &[
            ExampleDbErrors::invalid,
            ExampleDbErrors::already_exists,
            ExampleDbErrors::function_failure,
            ExampleDbErrors::authentication_error,
            ExampleDbErrors::unauthorized,
            ExampleDbErrors::config_error,
            ExampleDbErrors::unavailable,
            ExampleDbErrors::overloaded,
            ExampleDbErrors::is_bootstrapping,
            ExampleDbErrors::truncate_error,
            ExampleDbErrors::read_timeout,
            ExampleDbErrors::write_timeout,
            ExampleDbErrors::write_failure,
            ExampleDbErrors::unprepared,
            ExampleDbErrors::server_error,
            ExampleDbErrors::protocol_error,
            || ExampleDbErrors::other(2137),
        ];
        RequestReaction::forge_with_error_lazy_delay(
            Box::new(|| ERRORS[rand::thread_rng().next_u32() as usize % ERRORS.len()]()),
            delay,
        )
    }
}

impl Reaction for ResponseReaction {
    type Incoming = ResponseFrame;
    type Returning = RequestFrame;

    fn noop() -> Self {
        ResponseReaction {
            to_addressee: Some(Action {
                delay: None,
                msg_processor: None,
            }),
            to_sender: None,
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn drop_frame() -> Self {
        ResponseReaction {
            to_addressee: None,
            to_sender: None,
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn delay(time: Duration) -> Self {
        ResponseReaction {
            to_addressee: Some(Action {
                delay: Some(time),
                msg_processor: None,
            }),
            to_sender: None,
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn forge_response(f: Arc<dyn Fn(Self::Incoming) -> Self::Returning + Send + Sync>) -> Self {
        ResponseReaction {
            to_addressee: None,
            to_sender: Some(Action {
                delay: None,
                msg_processor: Some(f),
            }),
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn forge_response_with_delay(
        time: Duration,
        f: Arc<dyn Fn(Self::Incoming) -> Self::Returning + Send + Sync>,
    ) -> Self {
        ResponseReaction {
            to_addressee: None,
            to_sender: Some(Action {
                delay: Some(time),
                msg_processor: Some(f),
            }),
            drop_connection: None,
            feedback_channel: None,
        }
    }

    fn drop_connection() -> Self {
        ResponseReaction {
            to_addressee: None,
            to_sender: None,
            drop_connection: Some(None),
            feedback_channel: None,
        }
    }

    fn drop_connection_with_delay(time: Duration) -> Self {
        ResponseReaction {
            to_addressee: None,
            to_sender: None,
            drop_connection: Some(Some(time)),
            feedback_channel: None,
        }
    }

    fn with_feedback_when_performed(
        self,
        tx: mpsc::UnboundedSender<(Self::Incoming, Option<TargetShard>)>,
    ) -> Self {
        Self {
            feedback_channel: Some(tx),
            ..self
        }
    }
}

/// Describes what to with the given \<something\> (frame),
/// how to transform it and after what delay.
#[derive(Clone)]
pub struct Action<TFrom, TTo> {
    pub delay: Option<Duration>,
    pub msg_processor: Option<Arc<dyn Fn(TFrom) -> TTo + Send + Sync>>,
}

/// A rule describing what actions should the proxy perform
/// with the received request frame and on what conditions.
impl<TFrom, TTo> std::fmt::Debug for Action<TFrom, TTo> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Action")
            .field("delay", &self.delay)
            .field(
                "msg_processor",
                match self.msg_processor {
                    Some(_) => &"Some(<closure>)",
                    None => &"None",
                },
            )
            .finish()
    }
}

/// A rule describing what actions should the proxy perform
/// with the received request frame and on what conditions.
#[derive(Clone, Debug)]
pub struct RequestRule(pub Condition, pub RequestReaction);

/// A rule describing what actions should the proxy perform
/// with the received response frame and on what conditions.
#[derive(Clone, Debug)]
pub struct ResponseRule(pub Condition, pub ResponseReaction);

#[test]
fn condition_case_insensitive_matching() {
    let mut condition_matching =
        Condition::BodyContainsCaseInsensitive(Box::new(*b"cassandra'sInefficiency"));
    let mut condition_nonmatching =
        Condition::BodyContainsCaseInsensitive(Box::new(*b"cassandrasInefficiency"));
    let ctx = EvaluationContext {
        connection_seq_no: 42,
        opcode: FrameOpcode::Request(RequestOpcode::Options),
        frame_body: Bytes::from_static(b"\0\0x{0x223}Cassandra'sINEFFICIENCY\x12\x31"),
    };

    assert!(condition_matching.eval(&ctx));
    assert!(!condition_nonmatching.eval(&ctx));
}
