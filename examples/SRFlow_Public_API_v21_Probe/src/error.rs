use std::{error::Error, fmt};
// Local diagnostic bridge: retains the original Core error object.
impl Error for crate::core::internal_error::ScopeError {}
#[derive(Debug)]
pub enum BuildError {
    ForeignRef,
    InvisibleRef,
    DuplicateRootRef,
    InvalidIterationLimit,
    RetryLimitOverflow,
    DuplicateCase,
    DuplicateOtherwise,
    EmptyChoose,
}
impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for BuildError {}
#[derive(Debug)]
pub struct BodyFailure(pub(crate) Box<dyn Error>);
impl fmt::Display for BodyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for BodyFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[derive(Debug)]
pub struct RetryError {
    pub message: String,
    pub source: Option<Box<dyn Error>>,
}
impl RetryError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }
    pub fn caused_by<E: Error + 'static>(message: impl Into<String>, error: E) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(error)),
        }
    }
}
impl fmt::Display for RetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}
impl Error for RetryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}
#[derive(Debug, Default)]
pub struct IterBreak;
impl IterBreak {
    pub fn new() -> Self {
        Self
    }
}
#[derive(Debug)]
pub enum ControlSignal {
    Retry(RetryError),
    IterBreak(IterBreak),
}
#[derive(Debug)]
pub enum BodyError {
    Control(ControlSignal),
    Failure(BodyFailure),
}
impl BodyError {
    pub fn fail<E: Error + 'static>(error: E) -> Self {
        Self::Failure(BodyFailure(Box::new(error)))
    }
    pub fn retry(message: impl Into<String>) -> Self {
        Self::Control(ControlSignal::Retry(RetryError::new(message)))
    }
    pub fn iter_break() -> Self {
        Self::Control(ControlSignal::IterBreak(IterBreak))
    }
}
impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for BodyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Failure(e) => Some(e),
            Self::Control(ControlSignal::Retry(e)) => Some(e),
            _ => None,
        }
    }
}
#[derive(Debug)]
pub enum RunError {
    Definition(BuildError),
    Body(BodyFailure),
    RetryExhausted {
        max_retries: usize,
        attempts: usize,
        last_error: RetryError,
    },
    IterationLimitReached {
        max_iterations: usize,
        completed_iterations: usize,
    },
    UnhandledControl(ControlSignal),
    NoMatchingCase,
    Runtime(Box<dyn Error>),
}
impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for RunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Definition(e) => Some(e),
            Self::Body(e) => Some(e),
            Self::RetryExhausted { last_error, .. } => Some(last_error),
            Self::UnhandledControl(ControlSignal::Retry(error)) => Some(error),
            Self::Runtime(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}
#[derive(Debug)]
pub(crate) enum Signal {
    Control(ControlSignal),
    Terminal(RunError),
}
impl From<BodyError> for Signal {
    fn from(e: BodyError) -> Self {
        match e {
            BodyError::Control(c) => Self::Control(c),
            BodyError::Failure(f) => Self::Terminal(RunError::Body(f)),
        }
    }
}
impl From<crate::core::internal_error::ScopeError> for Signal {
    fn from(e: crate::core::internal_error::ScopeError) -> Self {
        Self::Terminal(RunError::Runtime(Box::new(e)))
    }
}
impl From<Signal> for RunError {
    fn from(s: Signal) -> Self {
        match s {
            Signal::Control(c) => Self::UnhandledControl(c),
            Signal::Terminal(e) => e,
        }
    }
}
