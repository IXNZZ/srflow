//! Flow：按声明顺序编排 Executable，并把已有数据连接成各个 child 的 Input。
//!
//! Flow 只负责顺序与数据连接，不承担业务计算。构建期由 [`FlowBuilder`] 完成
//! （取得 Input 的 [`Ref`] → 用 `then`／`then_move` 连接 child → 用 `output` 声明最终
//! Output），构建结果 [`Flow`] 才是可被 [`Runtime`] 执行的 Executable。

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::core::error::InvariantError;
use crate::core::reference::{FlowId, Ref, SlotId};
use crate::core::value_store::ValueStore;
use crate::core::{Executable, ExecutionError, Runtime};

/// Flow Input 固定占据的数据位置。
const INPUT_SLOT: SlotId = SlotId(0);

/// Flow 构建阶段的接线错误。
///
/// 这类错误发生在 `FlowBuilder` 上：连接本身不合法时，构建不会产出一个可执行的
/// [`Flow`]。它和 [`ExecutionError`] 的分工是：构建错误表示“这条连接根本不该成立”，
/// 执行错误表示“连接成立，但某次执行失败了”。
///
/// 后续任务会补充新的接线错误类别，因此本枚举标记为 `#[non_exhaustive]`：使用者匹配时必须
/// 保留 `_` 分支，新增变体不会破坏既有代码。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlowBuildError {
    /// 该 `Ref` 属于另一个 Flow。
    ///
    /// 跨 Flow 的数据只能通过 Executable 的 Input／Output 传递；即使两个 Flow 内部位置编号
    /// 相同、Rust 类型也相同，也必须失败。
    ForeignRef,
    /// 该数据位置的值已经被取走。
    ///
    /// [`FlowBuilder::then_move`] 或 [`FlowBuilder::output`] 会消费一个位置；之后任何读取都会
    /// 得到这个错误。需要让多个 child 共用同一个位置时，用 [`FlowBuilder::then`]。
    SourceAlreadyConsumed,
    /// 框架不变量被破坏（例如 `Ref` 指向当前 Flow 中不存在的位置）。
    Invariant(InvariantError),
}

impl fmt::Display for FlowBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignRef => {
                f.write_str("Ref 属于另一个 Flow：跨 Flow 的数据必须通过 Input／Output 传递")
            }
            Self::SourceAlreadyConsumed => f.write_str(
                "该数据位置的值已被取走：复用同一位置请用 then，最后一个消费者用 then_move",
            ),
            Self::Invariant(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for FlowBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invariant(error) => Some(error),
            _ => None,
        }
    }
}

/// Flow 的构建器：登记执行顺序与数据连接。
///
/// # 构建过程
///
/// ```
/// use srflow::{ExecutionError, FlowBuilder, Node, Runtime};
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
/// let mut flow = FlowBuilder::<String>::new();
/// let input = flow.input();
/// let length = flow.then_move(Length, input).unwrap();
/// let flow = flow.output(length).unwrap();
///
/// let runtime = Runtime::new();
/// let output = futures::executor::block_on(runtime.execute(&flow, String::from("abcd"))).unwrap();
/// assert_eq!(output, 4);
/// ```
///
/// # 顺序与数据依赖
///
/// `then`／`then_move` 的调用顺序就是执行顺序，与谁读取谁无关：即使某个 child 完全不使用前一步
/// 的 Output，它也不会被跳过、重排或并行。
///
/// # 读取方式
///
/// 同一个数据位置可以被多个 child 使用，但要显式选择读取方式：
///
/// - [`then`](Self::then)：复用读取。该位置之后仍可继续读取；本次执行按需要取副本，
///   最后一次读取直接拿走原值。要求该 Input 实现 [`Clone`]，因为复用意味着要复制出多份值。
/// - [`then_move`](Self::then_move)：消费读取。把该位置的值交给这一步，之后不能再读取它；
///   不要求 [`Clone`]，用于非 `Clone` 值或明确的一次性直通。
///
/// 位置是“先声明后使用”的：`Ref` 只能由 [`input`](Self::input) 与 `then`／`then_move` 的返回
/// 值产生，因此不存在指向尚未声明位置的 `Ref`。
///
/// # 数据所有权策略与代价
///
/// 每个位置的值在产生时存入本次执行的值存储。读取方式决定它怎么被交给 child：`then` 复用
/// 读取时按需复制、最后一次读取直接移动，`then_move`／`output` 直接移动。因此：
///
/// - 单消费者链路不复制业务值；
/// - 一个位置在本次执行中被读取 R 次就发生 R−1 次复制：`then` 的每次复用、以及最多一次由
///   `then_move` 或 `output` 发起的消费读取都计入 R；除最后一次读取直接移动原值外，其余每次
///   都要复制一份。注意 `output` 若也选中该位置，它同样算一次读取，会再多出一次复制。
///
/// 例如一个位置被两个 child 用 `then` 复用、再由 `output` 取出：共读取 3 次、复制 2 次；
/// 若 `output` 转而选中别的位置，则同两个 child 只读取 2 次、复制 1 次。
///
/// 与其他候选方案的比较：
///
/// | 候选 | 结果 | 为什么不采用 |
/// | --- | --- | --- |
/// | 所有读取都 `Clone` | API 形式最简单 | 单消费者链路也要复制大对象（SES 的长文本代价明显） |
/// | 值以 `Arc<T>` 保存 | 读取只递增引用计数 | child 的 Input 是 owned 的 `T`，共享后仍要交出一个 `T`，无法避免复制；除非把 `Arc<T>` 泄漏进业务侧 Input |
/// | 借用（把 Input 改成 `&T`） | 零复制 | 会改变 T01 的 Input 契约，并把生命周期传播到每个 Node 与组合型 Executable |
/// | 全部按所有权转移 | 零复制 | 值被取走后第二个 child 无法使用，破坏 Ref 可复用的设计语义 |
///
/// 这套策略保持 T01 的 owned Input 与 `Send` Future：`Flow` 与它的值存储仍然只保存
/// `Send` 的业务值。T03 的结构性装配同样只需要读取已有 `Ref`，届时按投影／组合复制所需的部分
/// 即可，类型擦除继续留在框架内部。
///
/// # 未完成的 Flow
///
/// `FlowBuilder` 本身不是 Executable：[`output`](Self::output) 未声明 Output 之前，构建结果
/// 不能被 [`Runtime`] 执行。
///
/// ```compile_fail
/// use srflow::{FlowBuilder, Runtime};
///
/// let runtime = Runtime::new();
/// let mut flow = FlowBuilder::<String>::new();
/// let input = flow.input();
/// # let _ = input;
/// // FlowBuilder 没有实现 Executable：未声明 Output 的构建结果无法执行。
/// let _ = runtime.execute(&flow, String::from("x"));
/// ```
pub struct FlowBuilder<I> {
    id: FlowId,
    slots: Vec<SlotMeta>,
    steps: Vec<Box<dyn Step>>,
    _marker: PhantomData<fn(I)>,
}

#[derive(Debug, Default, Clone, Copy)]
struct SlotMeta {
    /// 构建期登记的读取次数，执行期用于判断某次读取是否为最后一次。
    reads: u32,
    /// 值是否已经被 `then_move`／`output` 取走。
    consumed: bool,
}

impl<I> FlowBuilder<I> {
    /// 创建一个只包含 Flow Input 位置的构建器。
    pub fn new() -> Self {
        Self {
            id: FlowId::next(),
            slots: vec![SlotMeta::default()],
            steps: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// 取得代表本 Flow Input 的 `Ref`。
    pub fn input(&self) -> Ref<I> {
        Ref::new(self.id, INPUT_SLOT)
    }

    /// 加入一个 child，并声明它读取哪个位置（复用读取）。
    ///
    /// 该位置之后仍可继续读取（包括被 [`output`](Self::output) 选中）：执行时除最后一次读取外
    /// 都会克隆一份值给 child。
    ///
    /// # 错误
    ///
    /// - [`FlowBuildError::ForeignRef`]：`source` 属于另一个 Flow。
    /// - [`FlowBuildError::SourceAlreadyConsumed`]：该位置的值已被 `then_move`／`output` 取走。
    ///
    /// # 类型连接
    ///
    /// `source` 的类型必须与该 child 的 Input 一致；不一致无法编译。
    ///
    /// ```compile_fail
    /// use srflow::{ExecutionError, FlowBuilder, Node};
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
    /// let mut flow = FlowBuilder::<u32>::new();
    /// let input = flow.input();          // Ref<u32>
    /// // Length 需要 Ref<String>，类型不匹配，无法编译。
    /// let _ = flow.then(Length, input);
    /// ```
    ///
    /// 复用读取意味着要复制出多份值，因此 Input 必须实现 [`Clone`]；非 `Clone` 值请用
    /// [`then_move`](Self::then_move)。
    ///
    /// ```compile_fail
    /// use srflow::{ExecutionError, FlowBuilder, Node};
    ///
    /// struct Payload(String);            // 没有实现 Clone
    ///
    /// struct Consume;
    /// impl Node for Consume {
    ///     type Input = Payload;
    ///     type Output = usize;
    ///     async fn run(&self, input: Payload) -> Result<usize, ExecutionError> {
    ///         Ok(input.0.len())
    ///     }
    /// }
    ///
    /// let mut flow = FlowBuilder::<Payload>::new();
    /// let input = flow.input();
    /// // Payload 没有实现 Clone，`then` 无法编译；这里应该用 `then_move`。
    /// let _ = flow.then(Consume, input);
    /// ```
    pub fn then<E>(
        &mut self,
        executable: E,
        source: Ref<E::Input>,
    ) -> Result<Ref<E::Output>, FlowBuildError>
    where
        E: Executable + Send + Sync + 'static,
        E::Input: Clone + 'static,
        E::Output: 'static,
    {
        let slot = self.claim(&source, false)?;
        let target = self.next_slot();
        self.steps
            .push(Box::new(SharedStep::new(executable, slot, target)));
        Ok(Ref::new(self.id, target))
    }

    /// 加入一个 child，并把该位置的值交给它（消费读取）。
    ///
    /// 这一步之后不能再读取该位置；不要求 Input 实现 [`Clone`]，因此非 `Clone` 的业务值可以
    /// 沿“Input → child → … → Output”直通。
    ///
    /// # 错误
    ///
    /// - [`FlowBuildError::ForeignRef`]：`source` 属于另一个 Flow。
    /// - [`FlowBuildError::SourceAlreadyConsumed`]：该位置的值已经被取走。
    ///
    /// # 类型连接
    ///
    /// 与 [`then`](Self::then) 相同：`source` 的类型必须与该 child 的 Input 一致。
    ///
    /// # 示例：非 `Clone` 值直通
    ///
    /// ```
    /// use srflow::{ExecutionError, FlowBuilder, Node, Runtime};
    ///
    /// struct Payload(String);            // 没有实现 Clone
    ///
    /// struct Length;
    /// impl Node for Length {
    ///     type Input = Payload;
    ///     type Output = usize;
    ///     async fn run(&self, input: Payload) -> Result<usize, ExecutionError> {
    ///         Ok(input.0.chars().count())
    ///     }
    /// }
    ///
    /// let mut flow = FlowBuilder::<Payload>::new();
    /// let input = flow.input();
    /// let length = flow.then_move(Length, input).unwrap();
    /// let flow = flow.output(length).unwrap();
    ///
    /// let runtime = Runtime::new();
    /// let output = futures::executor::block_on(runtime.execute(&flow, Payload(String::from("abcd"))))
    ///     .unwrap();
    /// assert_eq!(output, 4);
    /// ```
    pub fn then_move<E>(
        &mut self,
        executable: E,
        source: Ref<E::Input>,
    ) -> Result<Ref<E::Output>, FlowBuildError>
    where
        E: Executable + Send + Sync + 'static,
        E::Input: 'static,
        E::Output: 'static,
    {
        let slot = self.claim(&source, true)?;
        let target = self.next_slot();
        self.steps
            .push(Box::new(ConsumingStep::new(executable, slot, target)));
        Ok(Ref::new(self.id, target))
    }

    /// 声明最终 Output，得到可执行的 [`Flow`]。
    ///
    /// 选中的位置可以是任意已经声明的位置，包括 Flow Input 本身；该位置的值会被取走，
    /// 因此它必须在所有复用读取之后。
    ///
    /// # 错误
    ///
    /// - [`FlowBuildError::ForeignRef`]：`source` 属于另一个 Flow。
    /// - [`FlowBuildError::SourceAlreadyConsumed`]：该位置的值已经被取走。
    pub fn output<O>(mut self, source: Ref<O>) -> Result<Flow<I, O>, FlowBuildError>
    where
        I: Send + 'static,
        O: Send + 'static,
    {
        let slot = self.claim(&source, true)?;
        let read_counts = self.slots.iter().map(|meta| meta.reads).collect();
        Ok(Flow {
            steps: self.steps,
            read_counts,
            output: slot,
            _marker: PhantomData,
        })
    }

    /// 校验归属与消费状态，并登记一次读取。
    fn claim<T>(&mut self, source: &Ref<T>, consume: bool) -> Result<SlotId, FlowBuildError> {
        if source.flow() != self.id {
            return Err(FlowBuildError::ForeignRef);
        }
        let slot = source.slot();
        let meta = self.slots.get_mut(slot.0).ok_or_else(|| {
            FlowBuildError::Invariant(InvariantError::new("Ref 指向本 Flow 之外的位置"))
        })?;
        if meta.consumed {
            return Err(FlowBuildError::SourceAlreadyConsumed);
        }
        meta.reads += 1;
        if consume {
            meta.consumed = true;
        }
        Ok(slot)
    }

    /// 为一个新产生的值分配数据位置。
    fn next_slot(&mut self) -> SlotId {
        let slot = SlotId(self.slots.len());
        self.slots.push(SlotMeta::default());
        slot
    }
}

impl<I> Default for FlowBuilder<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> fmt::Debug for FlowBuilder<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowBuilder")
            .field("input_type", &std::any::type_name::<I>())
            .field("steps", &self.steps.len())
            .field("slots", &self.slots.len())
            .finish()
    }
}

/// 已完成构建、可被 [`Runtime`] 执行的 Flow。
///
/// `Flow<I, O>` 对父级只暴露 `Input = I` 与 `Output = O`；内部各个 child 的类型、数量与
/// 中间数据位置都不出现在这个边界上。Flow 实现了 [`Executable`]，因此可以直接作为另一个
/// Flow 的 child（即 SubFlow），父级只能连接它的 Input／Output。
///
/// # 执行语义
///
/// - 按构建时的声明顺序依次执行每个 child，每个 child 的实际执行都重新经过
///   [`Runtime`]。
/// - 任一步骤返回执行错误时立即停止：后续 child 不执行，也不返回部分正常 Output。
///   已经发生的外部副作用不回滚。
/// - 每次执行创建独立的值存储，因此同一个 Flow 可以重复调用，甚至并发调用，调用之间不串值。
///
/// # 示例
///
/// ```
/// use srflow::{ExecutionError, FlowBuilder, Node, Runtime};
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
/// struct Double;
/// impl Node for Double {
///     type Input = usize;
///     type Output = usize;
///     async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
///         Ok(input * 2)
///     }
/// }
///
/// let mut flow = FlowBuilder::<String>::new();
/// let input = flow.input();
/// let length = flow.then_move(Length, input).unwrap();
/// let doubled = flow.then_move(Double, length).unwrap();
/// let flow = flow.output(doubled).unwrap();
///
/// let runtime = Runtime::new();
/// let output = futures::executor::block_on(runtime.execute(&flow, String::from("abcd"))).unwrap();
/// assert_eq!(output, 8);
/// ```
///
/// # Flow 只容纳可安全跨执行保存的 Executable
///
/// 为了把不同类型的 child 保存在同一个 Flow 中，child 类型需要 `Send + Sync + 'static`，
/// 且它们的 Input／Output 需要 `'static`。这比 T01 的 `Executable` 契约更严：`Node::run`
/// 只要求返回的 future 是 `Send`，因此一个 `!Sync` 的 Node 仍可单独经 Runtime 执行，却无法
/// 放进 Flow。下面先确认它可以独立执行：
///
/// ```
/// use std::cell::Cell;
/// use srflow::{ExecutionError, Node, Runtime};
///
/// /// 持有 `Cell`：`Send`，但不是 `Sync`。
/// struct NotSync {
///     hits: Cell<u32>,
/// }
///
/// impl Node for NotSync {
///     type Input = String;
///     type Output = usize;
///
///     // 显式返回 future 且不捕获 `&self`，因此 future 是 `Send`，Node 可经 Runtime 执行。
///     fn run(&self, input: String) -> impl Future<Output = Result<usize, ExecutionError>> + Send {
///         self.hits.set(self.hits.get() + 1);
///         async move { Ok(input.chars().count()) }
///     }
/// }
///
/// let runtime = Runtime::new();
/// let node = NotSync { hits: Cell::new(0) };
/// let output = futures::executor::block_on(runtime.execute(&node, String::from("abcd"))).unwrap();
/// assert_eq!(output, 4);
/// ```
///
/// 但同一个 Node 不能作为 Flow 的 child：`FlowBuilder` 额外要求 child `Sync`，而 `Cell`
/// 不满足。失败发生在 `then_move` 的约束处，而不是 `Node` 实现本身：
///
/// ```compile_fail
/// use std::cell::Cell;
/// use srflow::{ExecutionError, FlowBuilder, Node};
///
/// struct NotSync {
///     hits: Cell<u32>,
/// }
///
/// impl Node for NotSync {
///     type Input = String;
///     type Output = usize;
///
///     fn run(&self, input: String) -> impl Future<Output = Result<usize, ExecutionError>> + Send {
///         self.hits.set(self.hits.get() + 1);
///         async move { Ok(input.chars().count()) }
///     }
/// }
///
/// let mut flow = FlowBuilder::<String>::new();
/// let input = flow.input();
/// // Cell 不是 Sync，无法作为 Flow 的 child：`then_move` 的 `E: Sync` 约束不成立。
/// let _ = flow.then_move(NotSync { hits: Cell::new(0) }, input);
/// ```
pub struct Flow<I, O> {
    steps: Vec<Box<dyn Step>>,
    read_counts: Vec<u32>,
    output: SlotId,
    _marker: PhantomData<fn(I) -> O>,
}

impl<I, O> fmt::Debug for Flow<I, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flow")
            .field("input_type", &std::any::type_name::<I>())
            .field("output_type", &std::any::type_name::<O>())
            .field("steps", &self.steps.len())
            .finish()
    }
}

impl<I, O> Executable for Flow<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn execute(&self, runtime: &Runtime, input: I) -> Result<O, ExecutionError> {
        let mut store = ValueStore::new(&self.read_counts);
        store.insert(INPUT_SLOT, input);
        for step in &self.steps {
            step.run(runtime, &mut store).await?;
        }
        store.read_consuming::<O>(self.output)
    }
}

/// 一步的类型擦除表示：Flow 需要保存不同具体类型、不同 Input／Output 的 child。
type StepFuture<'a> = Pin<Box<dyn Future<Output = Result<(), ExecutionError>> + Send + 'a>>;

trait Step: Send + Sync {
    /// 读取本步 Input、执行 child、写回 Output。child 的执行必须重新经过 Runtime。
    fn run<'a>(&'a self, runtime: &'a Runtime, store: &'a mut ValueStore) -> StepFuture<'a>;
}

/// 复用读取的一步。
struct SharedStep<E> {
    executable: E,
    source: SlotId,
    target: SlotId,
}

impl<E> SharedStep<E> {
    fn new(executable: E, source: SlotId, target: SlotId) -> Self {
        Self {
            executable,
            source,
            target,
        }
    }
}

impl<E> Step for SharedStep<E>
where
    E: Executable + Send + Sync + 'static,
    E::Input: Clone + 'static,
    E::Output: 'static,
{
    fn run<'a>(&'a self, runtime: &'a Runtime, store: &'a mut ValueStore) -> StepFuture<'a> {
        Box::pin(async move {
            let input = store.read_shared::<E::Input>(self.source)?;
            let output = runtime.execute(&self.executable, input).await?;
            store.insert::<E::Output>(self.target, output);
            Ok(())
        })
    }
}

/// 消费读取的一步。
struct ConsumingStep<E> {
    executable: E,
    source: SlotId,
    target: SlotId,
}

impl<E> ConsumingStep<E> {
    fn new(executable: E, source: SlotId, target: SlotId) -> Self {
        Self {
            executable,
            source,
            target,
        }
    }
}

impl<E> Step for ConsumingStep<E>
where
    E: Executable + Send + Sync + 'static,
    E::Input: 'static,
    E::Output: 'static,
{
    fn run<'a>(&'a self, runtime: &'a Runtime, store: &'a mut ValueStore) -> StepFuture<'a> {
        Box::pin(async move {
            let input = store.read_consuming::<E::Input>(self.source)?;
            let output = runtime.execute(&self.executable, input).await?;
            store.insert::<E::Output>(self.target, output);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::core::Node;
    use crate::core::value_store::ValueStore;

    /// 记录自己是否被执行过的叶子。
    struct Counting {
        runs: Arc<AtomicU32>,
    }

    impl Node for Counting {
        type Input = usize;
        type Output = usize;

        async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            Ok(input)
        }
    }

    /// 定向验证：Input 解析不变量失败时，child 必须尚未启动。
    #[test]
    fn input_resolution_failure_stops_before_the_child_runs() {
        let runs = Arc::new(AtomicU32::new(0));
        let step = ConsumingStep::new(
            Counting {
                runs: Arc::clone(&runs),
            },
            SlotId(0),
            SlotId(1),
        );
        // 位置 0 登记了一次读取，但从来没有值写入：这是框架不变量被破坏的情形。
        let mut store = ValueStore::new(&[1, 0]);

        let error = futures::executor::block_on(step.run(&Runtime::new(), &mut store)).unwrap_err();

        assert!(matches!(error, ExecutionError::Invariant(_)));
        assert_eq!(runs.load(Ordering::SeqCst), 0, "child 不得被执行");
    }
}
