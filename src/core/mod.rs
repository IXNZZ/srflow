//! 核心层：统一的执行协议与执行入口。
//!
//! 普通使用场景可以直接从 crate 根部取得 [`Executable`]、[`Runtime`]、[`Node`] 与
//! [`ExecutionError`]；需要直接实现新的执行语义（组合型 `Executable`）时再进入本模块。
//!
//! T01 阶段本层只包含执行协议本身，尚未包含 Flow、Binding 与控制型 Executable。

pub mod error;
pub mod executable;
pub mod node;
pub mod runtime;

pub use error::ExecutionError;
pub use executable::Executable;
pub use node::Node;
pub use runtime::Runtime;
