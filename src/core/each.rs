//! Each：按输入顺序逐项执行同一个 Body，并按顺序收集结果。
//!
//! [`Each<B>`](Each) 的契约是 `Vec<I> → Vec<O>`，其中 `I`／`O` 来自 Body `B` 的
//! [`Executable::Input`]／[`Executable::Output`]：
//!
//! ```text
//! Each(Body): [I1, I2, I3] → [O1, O2, O3]
//!
//! Runtime.execute(Each, [I1, I2, I3])
//!   ├─ Runtime.execute(Body, I1) → O1
//!   ├─ Runtime.execute(Body, I2) → O2        // I1 的调用完成之后才开始
//!   └─ Runtime.execute(Body, I3) → O3
//! ```
//!
//! ```
//! use srflow::{Each, ExecutionError, Node, Runtime};
//!
//! struct Double;
//! impl Node for Double {
//!     type Input = u32;
//!     type Output = u32;
//!     async fn run(&self, input: u32) -> Result<u32, ExecutionError> {
//!         Ok(input * 2)
//!     }
//! }
//!
//! let each = Each::new(Double);
//! let runtime = Runtime::new();
//! let output = futures::executor::block_on(runtime.execute(&each, vec![1, 2, 3])).unwrap();
//! assert_eq!(output, vec![2, 4, 6]);
//! ```
//!
//! # 执行次数与顺序
//!
//! - 调用次数由输入集合长度限定：**全部正常完成时，每个 Item 恰好调用 Body 一次**；若某项出错，
//!   后续项不再执行。没有独立 `limit`；只想处理前 N 个 Item 时，应自行形成只含 N 个 Item 的输入集合。
//! - 顺序：第 k 个 Item 的异步调用**完成之后**才开始第 k+1 个。Each 不会先启动若干子调用再按顺序排列
//!   结果，也不做并发调度；使用 async 不会让它并行。
//! - 全部正常完成时，Output 与 Input 一一对应：不排序、不过滤、不去重，也不会因某项的正常业务
//!   Output 提前停止，或把某一项的 Output 混进下一项。
//!
//! # 空集合
//!
//! `[]` 是正常输入：Body 执行 0 次，返回 `[]`。空集合**不是错误**，也不产生默认 Item／Output。若业务
//! 认为“没有 Item”是非法的，应在进入 Each 之前用 Node 或明确的检查表达。
//!
//! # 错误
//!
//! 某一项返回 [`ExecutionError`] 时，Each 立即原样传播该错误：该项之后的 Item 不再启动，先前已经产生的
//! Output 也**不会**作为正常 `Vec<O>` 返回（没有“部分成功”载荷）。已经发生的外部副作用不会因此自动
//! 回滚，技术错误也不会被解释成“重试该项”或“跳过该项”。
//!
//! 业务上的否定结论（“不通过”“无候选”）是正常 Output，仍是一条 `O`，不会被自动当成错误或跳过。
//!
//! # Each 不建立跨项数据关系
//!
//! `Body(I2)` 的 Input 不包含 `O1`，`Body(I3)` 的 Input 不包含 `O2`：Each **本身**只建立
//! `Item → Output`。需要“下一项依赖上一项累积结果”时那属于 Iter，而不是用共享可变状态模拟。
//!
//! 这不等于各项绝对“彼此独立”：Body 仍然可以访问数据库、外部服务或自身的共享资源。准确的边界是
//! **Each 本身不建立跨轮 Output → Input 数据关系**。
//!
//! # 所有权与 bounds
//!
//! - `Vec<I>` 按值接收，Item 被**移动**给 Body，`O` 按值收集；`I`／`O`／Body 都不要求 `Clone`，也
//!   不存在为执行循环而复制 Item 的路径。
//! - **Body 需要 `Sync`**。Body 在同一次执行中被反复借用，该借用跨越逐项调用的 `.await`，而现有
//!   [`Executable::execute`] 的 Future 必须是 `Send`，因此即使单独经 [`Runtime`] 执行也要求借用对象
//!   为 `Sync`。这是本实现选定方案的明确约束：克隆 Body 会破坏“同一实例被复用”的语义，用锁包装
//!   Body 则引入锁粒度与异步 Guard 的取舍，都不为规避 `Sync` 而采用。
//! - `I`／`O` 只受 [`Executable`] 既有的 `Send` 约束，不被额外要求 `Sync`；它们不存放在 Each 中。
//!   作为父 Flow 的 child 时，另受 [`FlowBuilder::then`](crate::FlowBuilder::then) 的既有约束
//!   （`Each<B>: Send + Sync + 'static`、`Vec<I>`／`Vec<O>` 的 `'static`）。
//! - 每项 Body 的实际执行都重新经过父级传入的同一个 [`Runtime`]；Each 不直接调用
//!   [`Executable::execute`]，也不把逐项循环放进 Runtime。
//! - 结果容器是每次调用的局部状态：同一个 `Each` 定义可以重复调用，也可以被交叠轮询，互不干扰。

use std::fmt;

use crate::core::{Executable, ExecutionError, Runtime};

/// 对集合中的每个 Item 顺序执行同一个 Body，并按顺序收集结果。
///
/// 契约是 `Vec<I> → Vec<O>`，`I`／`O` 由 Body 的 [`Executable::Input`]／[`Executable::Output`] 决定；
/// Body 可以是 Node、Flow（SubFlow）或其他合法 [`Executable`]。语义、顺序与错误边界见本模块文档。
///
/// # 类型错误的接线无法编译
///
/// 元素类型由 Body 推导，父 Flow 的集合类型必须匹配：
///
/// ```compile_fail
/// use srflow::{Each, ExecutionError, FlowBuilder, Node};
///
/// struct Length;
/// impl Node for Length {
///     type Input = String;
///     type Output = usize;
///     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
///         Ok(input.chars().count())
///     }
/// }
///
/// let mut flow = FlowBuilder::<Vec<u32>>::new();
/// let input = flow.input();
/// // Body 的 Item 是 String，而这里提供的集合是 Vec<u32>。
/// let _ = flow.then_move(Each::new(Length), input);
/// ```
///
/// `Vec<O>` 交给下游时也必须与下游的 Input 一致：
///
/// ```compile_fail
/// use srflow::{Each, ExecutionError, FlowBuilder, Node};
///
/// struct Length;
/// impl Node for Length {
///     type Input = String;
///     type Output = usize;
///     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
///         Ok(input.chars().count())
///     }
/// }
///
/// struct Wrong;
/// impl Node for Wrong {
///     type Input = usize;
///     type Output = usize;
///     async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
///         Ok(input)
///     }
/// }
///
/// let mut flow = FlowBuilder::<Vec<String>>::new();
/// let input = flow.input();
/// let lengths = flow.then_move(Each::new(Length), input).unwrap();
/// // Each 的 Output 是 Vec<usize>，与 Wrong 的 Input 不匹配。
/// let _ = flow.then_move(Wrong, lengths);
/// ```
pub struct Each<B> {
    body: B,
}

impl<B> Each<B>
where
    B: Executable,
{
    /// 用给定 Body 构造 Each。
    ///
    /// Body 只实现 [`Executable`] 即可（普通业务方实现 [`Node`](crate::Node)，框架会自动接入）；
    /// 它在 Each 中被反复借用，因此**不需要 `Clone`**。
    ///
    /// ```
    /// use srflow::{Each, ExecutionError, Node, Runtime};
    ///
    /// struct Length;
    /// impl Node for Length {
    ///     type Input = String;
    ///     type Output = usize;
    ///     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
    ///         Ok(input.chars().count())
    ///     }
    /// }
    ///
    /// let each = Each::new(Length);
    /// let runtime = Runtime::new();
    /// let lengths = futures::executor::block_on(
    ///     runtime.execute(&each, vec![String::from("ab"), String::from("cde")]),
    /// )
    /// .unwrap();
    /// assert_eq!(lengths, vec![2, 3]);
    /// ```
    pub fn new(body: B) -> Self {
        Self { body }
    }
}

impl<B> fmt::Debug for Each<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Each")
            .field("body_type", &std::any::type_name::<B>())
            .finish_non_exhaustive()
    }
}

impl<B> Executable for Each<B>
where
    B: Executable + Sync,
{
    type Input = Vec<B::Input>;
    type Output = Vec<B::Output>;

    async fn execute(
        &self,
        runtime: &Runtime,
        items: Vec<B::Input>,
    ) -> Result<Vec<B::Output>, ExecutionError> {
        // 结果容器是本次调用的局部状态：不跨越调用，也不保存在 Each 定义里。
        let mut results = Vec::with_capacity(items.len());
        // 逐项串行：上一个 Item 的 Body 调用完成后才发起下一个。
        for item in items {
            results.push(runtime.execute(&self.body, item).await?);
        }
        Ok(results)
    }
}
