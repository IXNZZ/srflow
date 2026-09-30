//! `Retry`：以同一个业务 Input 有限次重做同一个 Body，直到正常 Output 表示不再需要重做。
//!
//! Retry 是控制型 [`Executable`]：它本身也是 Executable，对父 Flow 而言与 Node、SubFlow 走同一条
//! 连接入口 `flow.then(retry, binding)`。它只根据 Body 的**正常 Output** 中已有的控制信息决定是否
//! 再执行一轮；不做技术故障重试、退避或延迟。
//!
//! # 执行语义
//!
//! - **do-while**：只要 Retry 被执行，Body 至少运行一次。
//! - **有限**：`limit` 是 Body 的**总执行次数上限**（不是额外重试次数），默认 [`Retry::DEFAULT_LIMIT`]
//!   （8）。`limit` 用 [`NonZeroUsize`] 表示，`0` 在类型层面不可传入。
//! - **每轮同一个 Input**：每轮 Body 收到语义上相同的原始业务 Input，上一轮 Output 不会自动成为
//!   下一轮 Input；Retry 内部不保存跨轮的业务可变状态。
//! - **Condition**：每轮 Body 正常返回 `O` 后，Condition 恰好被调用一次（借用 `&O`），返回
//!   [`RetryDecision::Stop`] 就立即采用该轮 `O`；返回 [`RetryDecision::Retry`] 且尚未到上限就执行
//!   下一轮；返回 `Retry` 但已到上限则仍返回该轮正常 `O`。Condition 不是 Executable，不经过
//!   [`Runtime`]。
//! - **技术错误**：某轮 Body 返回 [`ExecutionError`] 时立即原样向外传播——该轮不调用 Condition、
//!   不执行后续轮次、不把它当作“需要重做”。Retry 不回滚已发生的外部副作用。
//!
//! # 上限耗尽不是错误
//!
//! 如果每轮 Output 都要求 `Retry`，直到用满 `limit`，Retry 返回**最后一次正常 Output**，不合成技术
//! 错误。这个结果在业务上意味着“未接受 / 需要人工处理”等，应由 `O` 自身表达。
//!
//! # 所有权与复制成本
//!
//! [`Executable::Input`] 按值传入，因此 Retry 只能收下 `I` 后为后续轮次提供 owned Input：重复执行
//! 路径**局部要求** `I: Clone`。这不改变 Body／Node／Flow 的 owned Input 契约，也不加 `Clone` 到
//! `Executable`／`Node` 或所有 Flow Input 上。**非 `Clone` 的 Input 不能经 Retry 执行**，即使
//! `limit = 1` 也一样。
//!
//! 采用“保留原始 Input，最后一个允许的轮次直接移动”的策略：若实际执行了 `n` 轮、上限为 `limit`，
//! 则 `I::clone` 被调用 **`n − [n == limit]`** 次。例如 `limit = 3`：第一轮停止复制 1 次，第二轮
//! 停止复制 2 次，执行满三轮复制 2 次；`limit = 1` 复制 0 次。这是因为只有在 `limit` 那一轮才能
//! 确定不会再有下一次执行——提前停止时，前 `n` 轮都必须各自持有一个克隆。这里计的是 `Clone` 调用
//! 次数，不承诺实际复制字节数，也不定义业务类型自定义 `Clone` 的语义。
//!
//! 每轮 `O` 不要求 `Clone`：Condition 借用 `&O`，最终 `O` 按值返回。
//!
//! # 类型约束
//!
//! - `Body: Executable + Sync`，`Condition: Fn(&O) -> RetryDecision + Sync`：执行 Future 必须 `Send`，
//!   而 `async fn execute(&self, ...)` 会捕获 `&self`，所以即使 Retry **单独**经 [`Runtime`] 执行也
//!   要求两者 `Sync`。这是本实现的局部约束，不是 `Executable` trait 对所有实现的普遍要求。
//! - 作为 Flow child 时还须满足既有的 `E: Send + Sync + 'static` 与 Input／Output `'static`。
//! - `I: Clone + Send`、`O: Send`；业务值均**不**要求 `Sync`。
//!
//! # 示例
//!
//! ```
//! use std::num::NonZeroUsize;
//! use srflow::{ExecutionError, Node, Retry, RetryDecision, Runtime};
//!
//! /// Body：生成一个候选，并把“是否接受”作为正常 Output 的一部分。
//! struct Generate;
//! impl Node for Generate {
//!     type Input = u32;
//!     type Output = (u32, bool);
//!     async fn run(&self, input: u32) -> Result<(u32, bool), ExecutionError> {
//!         Ok((input + 1, false))
//!     }
//! }
//!
//! let body = Generate;
//! // Condition 只读取已形成的判断字段。
//! let condition = |output: &(u32, bool)| {
//!     if output.1 { RetryDecision::Stop } else { RetryDecision::Retry }
//! };
//! let retry = Retry::with_limit(body, condition, NonZeroUsize::new(2).unwrap());
//!
//! let runtime = Runtime::new();
//! // 每轮都要求 retry，用满上限后返回最后一次正常 Output。
//! let output = futures::executor::block_on(runtime.execute(&retry, 40)).unwrap();
//! assert_eq!(output, (41, false));
//! ```
//!
//! Condition 必须是只读的局部判断：复杂业务判断应先由 Body 内的 Node 产出明确字段，Condition 只读取
//! 它。`Fn`／`&O` 不能从类型系统禁止内部可变性或复杂计算，这条边界由文档与评审约束。

use std::fmt;
use std::num::NonZeroUsize;

use crate::core::{Executable, ExecutionError, Runtime};

/// Retry 的 Condition 返回的控制结果。
///
/// [`Retry`] 固定使用本类型，而不是含义不明的裸 `bool`：`Retry` 要求再执行一轮，`Stop` 表示采用
/// 本轮 Output。两种结果对每一轮正常 Output 都会各被考虑一次，包括达到上限的最后一轮。
///
/// 这是一个封闭的两值契约：Retry 的执行逻辑对两个变体做穷尽匹配，因此将来若增加新的决策，必须
/// 同时显式定义它对 Retry 的影响，不会因为新增变体而悄悄改变语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision {
    /// 再执行一轮；若已到上限，则仍返回该轮正常 Output。
    Retry,
    /// 采用本轮 Output，立即结束。
    Stop,
}

/// 以同一个业务 Input 有限次重做同一个 Body。
///
/// 构造方式见 [`Retry::new`]（默认上限）与 [`Retry::with_limit`]（显式上限）。使用语义、所有权与
/// 复制成本见本模块文档。
///
/// # 例：作为父 Flow 的普通 child
///
/// ```
/// use std::num::NonZeroUsize;
/// use srflow::{ExecutionError, FlowBuilder, Node, Retry, RetryDecision, Runtime};
///
/// struct Make;
/// impl Node for Make {
///     type Input = String;
///     type Output = (String, bool);
///     async fn run(&self, input: String) -> Result<(String, bool), ExecutionError> {
///         Ok((input.clone(), input.len() >= 2))
///     }
/// }
///
/// struct Render;
/// impl Node for Render {
///     type Input = (String, bool);
///     type Output = String;
///     async fn run(&self, input: (String, bool)) -> Result<String, ExecutionError> {
///         Ok(input.0)
///     }
/// }
///
/// let mut flow = FlowBuilder::<String>::new();
/// let input = flow.input();
/// let retry = Retry::with_limit(
///     Make,
///     |output: &(String, bool)| {
///         if output.1 { RetryDecision::Stop } else { RetryDecision::Retry }
///     },
///     NonZeroUsize::new(3).unwrap(),
/// );
/// let rendered = flow.then(retry, input).unwrap();
/// let text = flow.then(Render, rendered).unwrap();
/// let flow = flow.output(text).unwrap();
///
/// let runtime = Runtime::new();
/// let output = futures::executor::block_on(runtime.execute(&flow, String::from("abcd"))).unwrap();
/// assert_eq!(output, "abcd");
/// ```
///
/// # 类型错误的连接无法编译
///
/// Condition 必须接受 Body 的 Output；不匹配时会作为 Executable 使用时被拒绝：
///
/// ```compile_fail
/// use srflow::{ExecutionError, Node, Retry, RetryDecision, Runtime};
///
/// struct Body;
/// impl Node for Body {
///     type Input = u32;
///     type Output = u32;
///     async fn run(&self, input: u32) -> Result<u32, ExecutionError> { Ok(input) }
/// }
///
/// // Condition 的参数应是 u32，这里写成 String，无法编译。
/// let retry = Retry::new(Body, |_: &String| RetryDecision::Stop);
/// let _ = futures::executor::block_on(Runtime::new().execute(&retry, 1));
/// ```
///
/// Retry 的重复执行路径要求 `I: Clone`，非 `Clone` 的 Input 无法编译：
///
/// ```compile_fail
/// use srflow::{ExecutionError, Node, Retry, RetryDecision, Runtime};
///
/// struct Payload(String); // 没有实现 Clone
///
/// struct Body;
/// impl Node for Body {
///     type Input = Payload;
///     type Output = u32;
///     async fn run(&self, _input: Payload) -> Result<u32, ExecutionError> { Ok(0) }
/// }
///
/// let retry = Retry::new(Body, |_: &u32| RetryDecision::Stop);
/// // Payload 不是 Clone，Retry 无法执行。
/// let _ = futures::executor::block_on(Runtime::new().execute(&retry, Payload(String::new())));
/// ```
///
/// 作为 Flow child 时，Retry 的 Input 必须与该位置的值类型一致：
///
/// ```compile_fail
/// use srflow::{ExecutionError, FlowBuilder, Node, Retry, RetryDecision};
///
/// struct Body;
/// impl Node for Body {
///     type Input = String;
///     type Output = u32;
///     async fn run(&self, input: String) -> Result<u32, ExecutionError> { Ok(input.len() as u32) }
/// }
///
/// let mut flow = FlowBuilder::<u32>::new();
/// let input = flow.input();          // Ref<u32>
/// let retry = Retry::new(Body, |_: &u32| RetryDecision::Stop);
/// // Retry 需要 String 的 Input，裸 Ref<u32> 不匹配，无法编译。
/// let _ = flow.then(retry, input);
/// ```
pub struct Retry<B, C> {
    body: B,
    condition: C,
    limit: NonZeroUsize,
}

impl<B, C> Retry<B, C> {
    /// 默认上限：Body 最多执行 8 次。
    pub const DEFAULT_LIMIT: NonZeroUsize = NonZeroUsize::new(8).unwrap();
}

impl<B, C> Retry<B, C>
where
    B: Executable,
    C: Fn(&B::Output) -> RetryDecision,
    B::Input: Clone,
{
    /// 用默认上限（8）构造 Retry。
    ///
    /// `body` 是需要重做的 Executable，`condition` 只根据每轮正常 Output 决定停止或重做。这两个
    /// 类型以及 `Body::Input: Clone` 的要求直接体现在构造器上，类型不匹配会在构造处被拒绝。
    pub fn new(body: B, condition: C) -> Self {
        Self {
            body,
            condition,
            limit: Self::DEFAULT_LIMIT,
        }
    }

    /// 用显式上限构造 Retry。
    ///
    /// `limit` 是 Body 的**总执行次数**上限。它用 [`NonZeroUsize`] 表示，因此 `0` 在类型层面就不可
    /// 传入（`NonZeroUsize::new(0)` 返回 `None`），不存在“Body 执行零次”的非法配置。
    ///
    /// ```
    /// use std::num::NonZeroUsize;
    /// use srflow::{ExecutionError, Node, Retry, RetryDecision, Runtime};
    ///
    /// struct Body;
    /// impl Node for Body {
    ///     type Input = u32;
    ///     type Output = u32;
    ///     async fn run(&self, input: u32) -> Result<u32, ExecutionError> { Ok(input + 1) }
    /// }
    ///
    /// // 上限 0 无法表示：
    /// assert!(NonZeroUsize::new(0).is_none());
    ///
    /// let retry = Retry::with_limit(
    ///     Body,
    ///     |_output: &u32| RetryDecision::Stop,
    ///     NonZeroUsize::new(1).unwrap(),
    /// );
    /// let output = futures::executor::block_on(Runtime::new().execute(&retry, 1)).unwrap();
    /// assert_eq!(output, 2);
    /// ```
    pub fn with_limit(body: B, condition: C, limit: NonZeroUsize) -> Self {
        Self {
            body,
            condition,
            limit,
        }
    }

    /// Body 的总执行次数上限。
    pub fn limit(&self) -> NonZeroUsize {
        self.limit
    }
}

impl<B, C> fmt::Debug for Retry<B, C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Retry")
            .field("body", &std::any::type_name::<B>())
            .field("condition", &std::any::type_name::<C>())
            .field("limit", &self.limit)
            .finish()
    }
}

impl<B, C> Executable for Retry<B, C>
where
    B: Executable + Sync,
    C: Fn(&B::Output) -> RetryDecision + Sync,
    B::Input: Clone,
{
    type Input = B::Input;
    type Output = B::Output;

    async fn execute(
        &self,
        runtime: &Runtime,
        input: B::Input,
    ) -> Result<B::Output, ExecutionError> {
        let limit = self.limit.get();
        // 保留原始 Input：只有在最后一个允许的轮次才能安全移动它。
        let mut retained = Some(input);
        let mut attempt: usize = 1;

        loop {
            let current = if attempt == limit {
                retained.take().expect("原始 Input 保留到最后一个允许轮次")
            } else {
                retained
                    .clone()
                    .expect("原始 Input 在最后一个允许轮次之前不会被取走")
            };

            // 每轮 Body 的实际执行都重新经过父级传入的 Runtime。
            let output = runtime.execute(&self.body, current).await?;

            // Body 正常返回时，Condition 恰好被调用一次（包括达到上限的最后一轮）。
            let decision = (self.condition)(&output);
            match decision {
                RetryDecision::Stop => return Ok(output),
                // 明确列出 `Retry`：将来新增决策时这里会因不是穷尽匹配而编译失败，
                // 从而强制显式定义新语义，而不是被默认当作“重试”。
                RetryDecision::Retry => {}
            }
            if attempt == limit {
                return Ok(output);
            }
            attempt += 1;
        }
    }
}
