//! SRFlow：以顺序 Flow 为核心的强类型执行与编排框架。
//!
//! 本 crate 提供 Core Runtime 的公开使用入口：声明业务 Data、接入函数／结构体
//! Node、组合 Flow／Match／Each／Loop、显式连接 [`DataRef<T>`](DataRef)，并以
//! [`Runtime::execute`](Runtime) 启动一次 Root Execution，取得 owned 输出或处理
//! 错误。全部能力经 crate 根的 re-export 使用，`core` 模块本身不对外。
//!
//! 支持范围（与任务验收一致）：
//! - 1～16 个非空 Root 与 Flow 输入，统一用 tuple 表达（单输入为 `(A,)`）；
//!   [`Unit`]／[`Data<O>`](Data)／[`Out2<O1, O2>`](Out2) 输出；
//! - 普通函数（同步／异步，0／1／2 输入，只产生 Data）、实现 [`NodeCall0`]／
//!   [`NodeCall1`]／[`NodeCall2`] 的具体结构体 Node、`Arc<具体 Node>`；
//! - Flow、Match、Each、Loop 的当前组合能力。
//!
//! 运行边界：stable、safe Rust；单线程顺序执行，不要求 `Send`；核心不绑定
//! executor，需要异步驱动时由调用方显式提供。
//!
//! # 最小示例
//!
//! ```
//! use futures::executor::block_on;
//! use srflow::{BodyError, Data, DataRef, Flow, FlowBuilder, Runtime, SyncFnSig};
//!
//! #[derive(Debug, PartialEq, Eq)]
//! struct Amount(u32);
//!
//! fn double(amount: &Amount) -> Result<Amount, BodyError> {
//!     Ok(Amount(amount.0 * 2))
//! }
//!
//! let (mut body, amount) = FlowBuilder::<(Amount,)>::start()?;
//! let doubled: DataRef<Amount> = body
//!     .then::<_, SyncFnSig<(Amount,), Data<Amount>>, _>(double as fn(&Amount) -> Result<Amount, BodyError>, amount)?;
//! let flow: Flow<(Amount,), Data<Amount>> = body.finish::<Data<Amount>, _>(doubled)?;
//! let out = block_on(Runtime::execute(&flow, (Amount(21),)))?;
//! assert_eq!(out, Amount(42));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! 更多用法见 `examples/minimal_flow.rs`（最小 Flow）与 `examples/controller_chain.rs`
//! （Each → Match 组合链、Loop Iter、构建期拒绝）。

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod api;
mod core;

pub use crate::api::{
    BuildError, EachBuilder, FlowBuilder, LoopBuilder, MatchBuilder, RunError, RunErrorKind,
    RunErrorStage, Runtime,
};
pub use crate::core::context::BodyError;
pub use crate::core::data_ref::DataRef;
pub use crate::core::each::{Each, EachOnly, EachShared};
pub use crate::core::flow::Flow;
pub use crate::core::loop_orchestrator::{
    Iter1, Iter2, Loop, LoopControl, LoopDecision, LoopShape, Retry1, Retry2,
};
pub use crate::core::match_orchestrator::Match;
pub use crate::core::node::{NodeCall0, NodeCall1, NodeCall2};
pub use crate::core::signature::{
    ArcNodeSig, AsyncFnSig, Data, NodeFut, NodeSig, OrchSig, Out2, OutputKind, SyncFnSig, Unit,
};
