use crate::core::{Executable, ExecutionError, Runtime};

/// SRFlow 的叶子业务执行单元。
///
/// 业务开发者只实现 `Node`：给出明确的 [`Input`](Node::Input) 与 [`Output`](Node::Output)，
/// 在 [`Node::run`] 中完成一个具体业务动作。框架为每个 `Node` 提供
/// [`Executable`] 的空白实现，因此 `Node` 可以直接交给
/// [`Runtime::execute`](crate::core::Runtime::execute)，不需要手写第二份 `Executable` 实现，
/// 也不需要在调用处包装成另一类对象。
///
/// # 叶子边界
///
/// Node 是叶子：它不编排其他 Executable。“如何执行其他步骤”（顺序、重试、路由、循环）属于
/// Flow 或控制型 Executable，不属于 Node。因此 [`Node::run`] 不接收 `Runtime`。
///
/// Node 可以显式持有配置、客户端或共享状态，但这些状态不能代替 `Input`／`Output` 表达的业务
/// 数据流：一次调用需要什么业务数据，必须由 `Input` 给出。
///
/// # 示例
///
/// ```
/// use srflow::{ExecutionError, Node, Runtime};
///
/// struct WordCount;
///
/// impl Node for WordCount {
///     type Input = String;
///     type Output = usize;
///
///     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
///         Ok(input.split_whitespace().count())
///     }
/// }
///
/// let runtime = Runtime::new();
/// let words = futures::executor::block_on(runtime.execute(&WordCount, String::from("a b c")))
///     .unwrap();
/// assert_eq!(words, 3);
/// ```
pub trait Node {
    /// 该 Node 一次调用所需的完整业务输入。
    type Input: Send;

    /// 该 Node 正常完成时产生的业务输出。
    type Output: Send;

    /// 执行一次业务操作。
    ///
    /// 业务上的否定结论（“不接受”“未通过”等）仍是正常输出，用 `Ok(...)` 返回；
    /// 只有技术执行失败才返回 [`ExecutionError`]。
    ///
    /// 实现者可以直接使用 `async fn`；返回的 future 必须是 `Send`。
    ///
    /// `async fn run(&self, ...)` 会把 `&self` 捕获进返回的 future，因此要求 `Self: Sync`。
    /// 只有不捕获 `self` 的实现（显式写 `-> impl Future<Output = ...> + Send`）才允许
    /// `!Sync` 的 Node 类型。
    fn run(
        &self,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, ExecutionError>> + Send;
}

/// 每个 `Node` 自动成为 `Executable`，因此无需手写适配代码即可经 Runtime 执行。
///
/// 该适配是 T01 中“普通使用者只实现 Node”的实现基础；它只做转发，不增加执行语义，
/// 也不把 `Runtime` 交给 Node。
impl<T: Node> Executable for T {
    type Input = T::Input;
    type Output = T::Output;

    fn execute(
        &self,
        _runtime: &Runtime,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output, ExecutionError>> + Send {
        self.run(input)
    }
}
