//! 核心层：统一的执行协议、执行入口，以及按顺序连接数据的 Flow。
//!
//! 普通使用场景可以直接从 crate 根部取得 [`Executable`]、[`Runtime`]、[`Node`]、
//! [`FlowBuilder`]、[`Flow`]、[`Ref`] 与 [`ExecutionError`]；需要直接实现新的执行语义
//! （组合型 `Executable`）时再进入本模块。
//!
//! T05 阶段本层包含执行协议、最小 Flow、Binding（整值读取、字段投影、多值组合与命名结构装配），
//! 以及两个控制型 Executable：`Retry`（有限重做）与 `Match`（依据已有路由值的单一路由）。
//! Each／Iter 仍属于后续任务。

pub mod binding;
pub mod error;
pub mod executable;
pub mod flow;
pub mod r#match;
pub mod node;
pub mod reference;
pub mod retry;
pub mod runtime;
mod value_store;

pub use binding::{Assemble, Binding, Consume, Field, consume};
pub use error::{ExecutionError, InvariantError};
pub use executable::Executable;
pub use flow::{Flow, FlowBuildError, FlowBuilder};
pub use r#match::{Match, MatchBuildError, MatchBuilder, NoMatch};
pub use node::Node;
pub use reference::Ref;
pub use retry::{Retry, RetryDecision};
pub use runtime::Runtime;
