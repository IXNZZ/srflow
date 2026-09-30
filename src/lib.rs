//! SRFlow —— 以强类型 Flow 为中心的系统执行框架。
//!
//! 本 crate 当前处于 T03：提供统一异步执行基础（[`Runtime`]、[`Executable`]、[`Node`]）、
//! 最小 Flow（[`FlowBuilder`]、[`Flow`]、[`Ref`]）以及 Binding（整值读取、字段投影、多值
//! 组合与命名结构装配）。控制型 Executable（Retry／Match／Each／Iter）尚未实现。
//!
//! # 四个角色
//!
//! - [`Runtime`]：统一的异步执行入口，所有实际执行都从 [`Runtime::execute`] 开始。
//! - [`Executable`]：统一的执行协议，一个具体执行边界有明确、强类型的 `Input` 与 `Output`。
//! - [`Node`]：叶子业务实现；业务开发者只实现 `Node`，框架自动把它接入 `Executable`。
//! - [`Flow`]：按声明顺序编排 Executable，并连接数据；它本身也是 `Executable`，因此可以
//!   直接作为另一个 Flow 的 child（SubFlow）。
//!
//! [`Binding`] 是 Flow 的数据连接机制，不是第五个角色：它在 child 启动前把已有数据装配成
//! child 的 Input，不进入 [`Runtime`]，也不产生执行记录。
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
//! # 编排：Flow
//!
//! `FlowBuilder` 收集执行顺序与数据连接，`output` 之后得到可执行的 [`Flow`]：
//!
//! ```
//! use srflow::{ExecutionError, FlowBuilder, Node, Runtime};
//!
//! struct Length;
//! impl Node for Length {
//!     type Input = String;
//!     type Output = usize;
//!     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
//!         Ok(input.chars().count())
//!     }
//! }
//!
//! struct Double;
//! impl Node for Double {
//!     type Input = usize;
//!     type Output = usize;
//!     async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
//!         Ok(input * 2)
//!     }
//! }
//!
//! let mut flow = FlowBuilder::<String>::new();
//! let input = flow.input();
//! let length = flow.then_move(Length, input).unwrap();
//! let doubled = flow.then_move(Double, length).unwrap();
//! let flow = flow.output(doubled).unwrap();
//!
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(runtime.execute(&flow, String::from("abcd"))).unwrap();
//! assert_eq!(output, 8);
//! ```
//!
//! 同一个位置要被多个步骤使用，用 [`FlowBuilder::then`]（复用读取，要求该 Input 实现
//! [`Clone`]）；把值交给某一步之后不再读取，用 [`consume`] 或 [`FlowBuilder::then_move`]
//! （消费读取，不要求 [`Clone`]）。`then` 的执行顺序就是执行顺序，与谁读取谁无关。
//!
//! # 装配 Input：Binding
//!
//! `then(executable, binding)` 的第二个参数描述 child 的 Input 如何从当前 Flow 已有数据中
//! 形成。除整值 [`Ref`] 外，还可以用 [`field!`] 投影字段、用 tuple 组合多个来源、用
//! [`bind!`] 命名装配业务结构（也可嵌套）：
//!
//! ```
//! use srflow::{bind, field, ExecutionError, FlowBuilder, Node, Runtime};
//!
//! // 根结构没有实现 Clone：字段投影仍然可以读取它。
//! struct Story {
//!     plan: String,
//!     background: String,
//! }
//!
//! struct ProgressionInput {
//!     plan: String,
//!     key: u32,
//!     background: String,
//! }
//!
//! struct MakeKey;
//! impl Node for MakeKey {
//!     type Input = String;
//!     type Output = u32;
//!     async fn run(&self, input: String) -> Result<u32, ExecutionError> {
//!         Ok(input.len() as u32)
//!     }
//! }
//!
//! struct Progression;
//! impl Node for Progression {
//!     type Input = ProgressionInput;
//!     type Output = String;
//!     async fn run(&self, input: ProgressionInput) -> Result<String, ExecutionError> {
//!         Ok(format!("{}/{}/{}", input.plan, input.key, input.background))
//!     }
//! }
//!
//! let mut flow = FlowBuilder::<Story>::new();
//! let story = flow.input();
//! let key = flow.then(MakeKey, field!(story.plan)).unwrap();
//! let input = bind!(ProgressionInput {
//!     plan: field!(story.plan),
//!     key: key,
//!     background: field!(story.background),
//! });
//! let result = flow.then(Progression, input).unwrap();
//! let flow = flow.output(result).unwrap();
//!
//! let runtime = Runtime::new();
//! let story = Story {
//!     plan: String::from("plan"),
//!     background: String::from("bg"),
//! };
//! let output = futures::executor::block_on(runtime.execute(&flow, story)).unwrap();
//! assert_eq!(output, "plan/4/bg");
//! ```
//!
//! Binding 只能读取、投影、组合与构造当前 Flow 已有的值，不承担业务计算；判断与计算属于
//! [`Node`]。形状、封闭性与 Flow 归属见 [`Binding`]、[`field!`]、[`bind!`]、[`consume`]。
//!
//! # Binding 的结构性边界
//!
//! [`Binding`] 是封闭 trait：它继承一个不可命名的私有 supertrait，业务侧无法为自定义类型实现
//! 它，因此不存在把任意计算实现成 Binding 的入口。字段投影与命名装配只通过 [`field!`]、
//! [`bind!`] 表达。
//!
//! 为让宏在业务 crate 中可用，框架保留了两个 `#[doc(hidden)]` 的最小构造入口
//! （[`__project_field`]、[`__assemble`]）。它们是**唯一的开放面**，而且都不封闭：
//!
//! - `__project_field` 的回调签名是 `fn(&Root) -> &F`。Rust 不能要求返回值**来自**根：忽略根、
//!   返回 `&'static F` 的回调同样满足该签名，因此直接调用这个入口可以**把当前 Flow 之外的
//!   数据注入下游**（已用外部探针验证：`String` 根的 Flow，回调返回静态 `u32`，下游收到该值）；
//! - `__assemble` 的回调是 `Fn(已解析字段) -> 结构`，直接调用它可以在回调里写任意计算。
//!
//! 两者都是本阶段**已知的剩余漏洞**：由本文档与代码评审承担，类型系统没有封死它们。`field!`／
//! `bind!` 宏本身只生成字段路径与字段装配，不会注入；但宏参数里写出满足 Binding 约束的表达式
//! 仍可能绕过约定，同样属于评审红线。因此不要直接调用这两个入口，请使用 [`field!`]、[`bind!`]。
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
//! 可运行版本见 `examples/`。
//!
//! # 错误与业务结果
//!
//! 业务上的否定结论（`accepted: false`、“未通过”、“无候选”等）是正常 `Output`，用
//! `Ok(...)` 返回；技术执行失败是 [`ExecutionError::Failed`]，框架不变量被破坏是
//! [`ExecutionError::Invariant`]。错误按 fail-fast 规则向调用方传播并保留来源，Runtime 不自动
//! 重试、跳过、回滚或切换备用路径。
//!
//! 接线本身不合法（跨 Flow 的 `Ref`、重复取走同一个位置、同一 Binding 内既消费又复用同一位置）
//! 不会等到执行期才失败：它在构建期以 [`FlowBuildError`] 拒绝，也不会产出一个可执行的 Flow。
//!
//! # 执行不变量
//!
//! - 所有 Executable 的实际执行都经过 [`Runtime::execute`]，包括 Flow 内部的每一个 child。
//! - 组合型 Executable 调用 child 时必须重新经过同一个 Runtime：`Executable::execute` 接收
//!   父级传入的 `&Runtime`，child 只能用它来执行。
//! - [`Node`] 是叶子：它不接收 Runtime，也不能编排其他 Executable。
//! - [`Flow`] 只定义顺序与数据连接，不做业务判断与计算。
//! - `Input` 在一次调用中是只读业务事实；跨调用传递业务数据必须通过显式的 `Input`／`Output`。
//! - `Ref` 属于特定 Flow，只读；Flow 内部位置不会越过 Flow 边界暴露给父级。
//!
//! # 实现决策与约束
//!
//! - **异步**：[`Executable::execute`] 与 [`Node::run`] 返回 `impl Future<Output = ...> + Send`，
//!   实现者直接使用 `async fn` 即可。要求 `Send` 是为了让执行树能在多线程 executor 上运行；
//!   代价是 `Input`／`Output`（关联类型已声明 `Send`）以及实现者跨 `await` 持有的状态也需要
//!   `Send`。用 `async fn` 实现时 `&self` 会被捕获，因此还需要 `Self: Sync`。
//! - **不绑定 executor**：核心只表达 `Future`，普通依赖为空；文档、测试与示例用
//!   `futures::executor::block_on` 驱动，那只是开发依赖，使用者需要自己选择并添加 executor。
//! - **错误转换**：外部错误进入 [`ExecutionError`] 需要显式转换
//!   （`外部调用().map_err(ExecutionError::new)?`）；取舍记录见 [`ExecutionError`] 的文档。
//! - **`Input` 按值传入**：语义上仍然只读，避免把实现绑死在某个生命周期上；Flow 的数据复用由
//!   构建期选择的读取方式决定（见 [`FlowBuilder`]）。
//! - **Flow 的数据所有权**：每个位置的值在产生时以类型擦除的形式存入本次执行的值存储；
//!   复用读取按需要克隆，最后一次复用/消费读取直接移动原值；字段投影借用根、只复制目标字段，
//!   因此根不必实现 `Clone`（字段需要）。单消费者链路不复制业务值，复用链路只复制真正需要
//!   多份的那几次；非 `Clone` 值可以经 [`consume`]／[`FlowBuilder::then_move`]／`output` 直通。
//! - **Binding 的结构性边界**：[`Binding`] 是封闭 trait，只由框架定义的结构类型实现，公开 API
//!   不提供 `map(any_function)`；投影与装配经 [`field!`]、[`bind!`] 表达。但为让宏可用，底层
//!   保留了两个 `#[doc(hidden)]` 的公开构造入口，二者**都不封闭**：`__project_field` 可注入
//!   根外数据，`__assemble` 可在回调里写任意计算。这是本阶段已知、需由评审约束的剩余漏洞，
//!   详见“Binding 的结构性边界”一节。
//! - **Flow 能容纳的 Executable**：为了保存在同一个 Flow 里，child 需要 `Send + Sync +
//!   'static`，Input／Output 需要 `'static`。这比 T01 的 `Executable` 契约更严，但没有修改
//!   T01 的公共 trait：不属于这一范围的 Executable 仍可单独经 Runtime 执行。
//! - **trait 不是 object-safe**：`Executable` 使用 RPITIT（`impl Future`），因此不能构造
//!   `dyn Executable`。Flow 内部的类型擦除发生在框架自己的适配层，业务侧看不到 `dyn`、`Any`
//!   或弱类型值。
//! - **MSRV**：`rust-version = 1.85`，下限来自 edition 2024；执行协议本身只需要 Rust 1.75
//!   的 RPITIT 与 `+ Send`。理由与依赖选择记录在 `Cargo.toml` 注释中。
//!
//! 普通使用场景从 crate 根部取得上述公共契约；直接使用核心接口的扩展作者可以从 [`core`]
//! 模块进入。

pub mod core;

pub use crate::core::{
    Assemble, Binding, Consume, Executable, ExecutionError, Field, Flow, FlowBuildError,
    FlowBuilder, InvariantError, Node, Ref, Runtime, consume,
};

// 供 `field!`／`bind!` 宏展开调用；不是公开契约，请勿直接使用。
#[doc(hidden)]
pub use crate::core::binding::{__assemble, __project_field};
