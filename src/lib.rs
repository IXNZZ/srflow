//! SRFlow —— 以强类型 Flow 为中心的系统执行框架。
//!
//! 本 crate 当前处于 T01：只交付统一异步执行基础（[`Runtime`]、[`Executable`]、[`Node`]）。
//! Flow、Ref、Binding、Retry、Match、Each、Iter 尚未实现，也不属于本阶段范围。
//!
//! # 三个角色
//!
//! - [`Runtime`]：统一的异步执行入口，所有实际执行都从 [`Runtime::execute`] 开始。
//! - [`Executable`]：统一的执行协议，一个具体执行边界有明确、强类型的 `Input` 与 `Output`。
//! - [`Node`]：叶子业务实现；业务开发者只实现 `Node`，框架自动把它接入 `Executable`。
//!
//! # 快速上手：只实现 Node
//!
//! ```
//! use srflow::{ExecutionError, Node, Runtime};
//!
//! /// 计算一个整数的平方。
//! struct Square;
//!
//! impl Node for Square {
//!     type Input = u32;
//!     type Output = u32;
//!
//!     async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
//!         Ok(input * input)
//!     }
//! }
//!
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(runtime.execute(&Square, 7)).unwrap();
//! assert_eq!(output, 49);
//! ```
//!
//! 文档与示例里的 `futures::executor::block_on` 只是用来驱动异步代码：`srflow` 本身不依赖
//! 任何 executor，使用者需要在自己的项目里选择并添加一个（`futures` 只是本仓库的开发依赖，
//! 不会随 `srflow` 提供给使用者）。
//!
//! # 扩展：实现组合型 Executable
//!
//! 需要新增执行语义（而不是新增业务操作）时直接实现 [`Executable`]。组合型实现通过父级传入的
//! [`Runtime`] 执行 child，因此每一个 child 的实际调用仍然经过同一个执行入口：
//!
//! ```no_run
//! use srflow::{ExecutionError, Executable, Node, Runtime};
//!
//! struct WordCount;
//!
//! impl Node for WordCount {
//!     type Input = String;
//!     type Output = usize;
//!
//!     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
//!         Ok(input.split_whitespace().count())
//!     }
//! }
//!
//! /// 对父级只暴露 `String → usize`，内部通过 Runtime 调用 child。
//! struct WordCountDoubled;
//!
//! impl Executable for WordCountDoubled {
//!     type Input = String;
//!     type Output = usize;
//!
//!     async fn execute(&self, runtime: &Runtime, input: String) -> Result<usize, ExecutionError> {
//!         let words = runtime.execute(&WordCount, input).await?;
//!         Ok(words * 2)
//!     }
//! }
//! ```
//!
//! 可运行版本见 `examples/node_only.rs` 与 `examples/composite_executable.rs`。
//!
//! # 错误与业务结果
//!
//! 业务上的否定结论（`accepted: false`、“未通过”、“无候选”等）是正常 `Output`，用
//! `Ok(...)` 返回；技术执行失败（外部依赖失败、child 失败等）是 [`ExecutionError`]，用
//! `Err(...)` 返回。错误按 fail-fast 规则向调用方传播并保留来源，Runtime 不自动重试、跳过、
//! 回滚或切换备用路径。
//!
//! # 执行不变量
//!
//! - 所有 Executable 的实际执行都经过 [`Runtime::execute`]。
//! - 组合型 Executable 调用 child 时必须重新经过同一个 Runtime：`Executable::execute` 接收
//!   父级传入的 `&Runtime`，child 只能用它来执行。
//! - [`Node`] 是叶子：它不接收 Runtime，也不能编排其他 Executable。
//! - `Input` 在一次调用中是只读业务事实；跨调用传递业务数据必须通过显式的 `Input`／`Output`。
//!
//! # 实现决策与约束
//!
//! - **异步**：[`Executable::execute`] 与 [`Node::run`] 返回 `impl Future<Output = ...> + Send`，
//!   实现者直接使用 `async fn` 即可。要求 `Send` 是为了让执行树能在多线程 executor 上运行；
//!   代价是 `Input`／`Output`（关联类型已声明 `Send`）以及实现者跨 `await` 持有的状态也需要
//!   `Send`。这同样意味着后续的异构组合（Flow 等）只能保存 `Send` 的业务值。
//! - **不绑定 executor**：核心只表达 `Future`，普通依赖为空；文档、测试与示例用
//!   `futures::executor::block_on` 驱动，那只是开发依赖，使用者需要自己选择并添加 executor。
//! - **错误转换**：外部错误进入 [`ExecutionError`] 需要显式转换
//!   （`外部调用().map_err(ExecutionError::new)?`）；取舍记录见 [`ExecutionError`] 的文档。
//! - **`Input` 按值传入**：语义上仍然只读，避免把实现绑死在某个生命周期上；需要复用同一份
//!   数据时由实现显式 `clone` 或共享，而不是由框架隐式复制业务数据。
//! - **trait 不是 object-safe**：`Executable` 使用 RPITIT（`impl Future`），因此不能构造
//!   `dyn Executable`。需要把不同具体类型的 Executable 放进同一组合边界时（后续的 Flow 等），
//!   类型擦除应发生在核心内部的适配层，而不是要求业务侧使用 `dyn`、`Any` 或弱类型值。
//! - **MSRV**：`rust-version = 1.85`，下限来自 edition 2024；执行协议本身只需要 Rust 1.75
//!   的 RPITIT 与 `+ Send`。理由与依赖选择记录在 `Cargo.toml` 注释中。
//!
//! 普通使用场景从 crate 根部取得 [`Runtime`]、[`Node`]、[`Executable`] 与 [`ExecutionError`]；
//! 直接使用核心接口的扩展作者可以从 [`core`] 模块进入。

pub mod core;

pub use crate::core::{Executable, ExecutionError, Node, Runtime};
