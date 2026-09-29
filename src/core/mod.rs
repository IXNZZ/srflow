//! 核心层：统一的执行协议、执行入口，以及按顺序连接数据的 Flow。
//!
//! 普通使用场景可以直接从 crate 根部取得 [`Executable`]、[`Runtime`]、[`Node`]、
//! [`FlowBuilder`]、[`Flow`]、[`Ref`] 与 [`ExecutionError`]；需要直接实现新的执行语义
//! （组合型 `Executable`）时再进入本模块。
//!
//! T02 阶段本层包含执行协议与最小 Flow（整值 `Ref<T>` 连接）。字段投影、多值组合与命名结构
//! 装配，以及控制型 Executable，仍属于后续任务。

pub mod error;
pub mod executable;
pub mod flow;
pub mod node;
pub mod reference;
pub mod runtime;
mod value_store;

pub use error::{ExecutionError, InvariantError};
pub use executable::Executable;
pub use flow::{Flow, FlowBuildError, FlowBuilder};
pub use node::Node;
pub use reference::Ref;
pub use runtime::Runtime;
