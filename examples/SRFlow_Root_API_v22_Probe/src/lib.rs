//! Minimal v2.2 Root API supplement. Uses the existing concrete Runtime and Data Core.
//! The public entry is Runtime::execute(closure, owned_input); no Workflow or Flow::build.
//! Only the independent consumer tests and root_minimal example are evidence here.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![allow(dead_code)] // Untargeted copied legacy builders are retained, not exercised.

mod core;

// 让本 crate 内部（含 doctest 展开）以 `::srflow::…` 引用自身；
// derive 宏对“库自身”统一生成这一路径。
extern crate self as srflow;

// ---------------------------------------------------------------- 新契约的 crate 根导出

pub use crate::core::engine::choice::Choice;
pub use crate::core::engine::data::Data;
pub use crate::core::engine::error::{
    BodyError, BodyFailure, BuildError, ControlSignal, IterBreak, RetryError, RunError,
    RuntimeError,
};
pub use crate::core::engine::node::Node;
#[doc(hidden)]
pub use crate::core::engine::node::{Callable, FunctionCall, StructCall};
pub use crate::core::engine::probe_api::Flow;
pub use crate::core::engine::runtime::Runtime;
pub use crate::core::engine::shape::{
    EachShape, InputSpec, NodeOutput, Query, QueryType, Ref, RefShape, RootInput,
};
pub use srflow_data_derive::Data;
