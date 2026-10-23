//! SRFlow：以顺序 Flow 为核心的强类型执行与编排框架。
//!
//! 本 crate 提供新契约的公开使用入口：声明业务 [`Data`]、接入函数／结构体 [`Node`]、
//! 用 [`Flow::then`]／[`Flow::then_with_output`]／[`Flow::chain`]／[`Flow::choose`]／
//! [`Flow::retry`]／[`Flow::iter`]
//! 组合 Definition，并以
//! [`Runtime::execute`] 启动一次 Root Execution，取得 owned 输出或 [`RunError`]。
//!
//! 当前可执行范围：Node、Chain、Each、Choose、Retry 与 Iter 组成的主链——冻结的全部
//! Root／Node／Body Shape、嵌套 Chain／Each、值匹配 Choose（[`Choice`]）、受预算约束的
//! Retry 与 Iter 控制器（含成组状态推进与 IterBreak 交付）、深层祖先捕获与实际逐层
//! Import、调用边界输出交接，以及 Root owned 整组提取。旧公开模型（`legacy` 命名空间与
//! 旧的 Builder／Flow／Match／Each／Loop／NodeCall 协议）已在 WE-T06 B 阶段退出。
//!
//! # 最小示例
//!
//! ```
//! use futures::executor::block_on;
//! use srflow::{BodyError, Data, Node, Query, Ref, Runtime};
//!
//! #[derive(Data)]
//! struct Amount(u32);
//!
//! async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
//!     Ok(Amount(query.get().0 * 2))
//! }
//!
//! struct AddOne;
//! impl Node for AddOne {
//!     type Input = Amount;
//!     type Output = Amount;
//!
//!     async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
//!         Ok(Amount(query.get().0 + 1))
//!     }
//! }
//!
//! let output = block_on(async {
//!     let runtime = Runtime::new();
//!     let add = AddOne;
//!     // Root 闭包固定返回 `()`：业务输出由 `output_with` 显式声明。
//!     let output: Amount = runtime
//!         .execute(
//!             |flow, amount| {
//!                 let doubled = flow.then_with_output(double, amount);
//!                 // 取输出形式：Body 内先声明，再返回父域 Ref。
//!                 let refs: Ref<Amount> = flow.chain_output(|sub| {
//!                     let produced = sub.then_with_output(&add, doubled);
//!                     sub.output_with(produced);
//!                 });
//!                 flow.output_with(refs);
//!             },
//!             Amount(21),
//!         )
//!         .await
//!         .unwrap();
//!     output
//! });
//! assert_eq!(output.0, 43);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

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
pub use crate::core::engine::flow::Flow;
pub use crate::core::engine::node::Node;
#[doc(hidden)]
pub use crate::core::engine::node::{
    AutoCallable, AutoFunction, Callable, FunctionCall, StructCall,
};
pub use crate::core::engine::runtime::Runtime;
pub use crate::core::engine::shape::{
    EachShape, InputSpec, NodeOutput, Query, QueryType, Ref, RefShape, RootInput,
};
pub use srflow_data_derive::Data;
