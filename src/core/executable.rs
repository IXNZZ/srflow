use crate::core::{ExecutionError, Runtime};

/// SRFlow 的统一执行协议。
///
/// 一个 `Executable` 定义一条明确的执行边界：
///
/// ```text
/// Input
///   ↓
/// Executable
///   ↓
/// Output
/// ```
///
/// 具体类型拥有明确、强类型的 [`Input`](Executable::Input) 与 [`Output`](Executable::Output)；
/// 父级只需要知道这组契约，不需要知道内部是否包含子步骤、重试或迭代。
///
/// # 两种实现形态
///
/// - **叶子型**：自己完成业务操作。业务开发者实现 [`Node`](crate::core::Node) 即可，
///   框架提供的空白实现会把每个 `Node` 接入本 trait。
/// - **组合型**：通过其他 `Executable` 完成自身语义。这类实现必须通过
///   [`Runtime::execute`](crate::core::Runtime::execute) 调用 child，不得直接调用 child 的
///   [`Executable::execute`]；`execute` 收到父级传入的 `&Runtime`，child 只能用它执行。
///
/// # 执行语义
///
/// - `input` 在本次调用中是只读业务事实：结果必须通过返回值给出，不得靠修改调用方数据传递。
/// - 技术失败返回 [`ExecutionError`]；业务上的否定结论是正常 `Output`。
/// - 同一个 `Executable` 可被多次调用；一次调用的瞬时数据不会自动成为下一次调用的 `Input`。
pub trait Executable {
    /// 该 Executable 一次调用所需的完整输入。
    type Input: Send;

    /// 该 Executable 正常完成时产生的完整输出。
    type Output: Send;

    /// 使用给定 `Runtime` 执行一次。
    ///
    /// 实现者可以直接使用 `async fn`；返回的 future 必须是 `Send`。用 `async fn` 实现时
    /// `&self` 会被捕获进 future，因此要求 `Self: Sync`（组合型 Executable 通常也要求
    /// `Runtime: Sync`，它是无状态的）。
    ///
    /// 组合型实现必须把 child 交给传入的 `runtime` 执行，例如：
    ///
    /// ```no_run
    /// # use srflow::{ExecutionError, Executable, Node, Runtime};
    /// # struct Child;
    /// # impl Node for Child {
    /// #     type Input = u32;
    /// #     type Output = u32;
    /// #     async fn run(&self, input: u32) -> Result<u32, ExecutionError> { Ok(input) }
    /// # }
    /// struct Parent;
    ///
    /// impl Executable for Parent {
    ///     type Input = u32;
    ///     type Output = u32;
    ///
    ///     async fn execute(&self, runtime: &Runtime, input: u32) -> Result<u32, ExecutionError> {
    ///         runtime.execute(&Child, input).await
    ///     }
    /// }
    /// ```
    fn execute(
        &self,
        runtime: &Runtime,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, ExecutionError>> + Send;
}
