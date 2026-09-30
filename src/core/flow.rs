//! Flow：按声明顺序编排 Executable，并把已有数据连接成各个 child 的 Input。
//!
//! Flow 只负责顺序与数据连接，不承担业务计算。构建期由 [`FlowBuilder`] 完成
//! （取得 Input 的 [`Ref`] → 用 [`then`](FlowBuilder::then) 按 Binding 连接 child →
//! 用 [`output`](FlowBuilder::output) 声明最终 Output），构建结果 [`Flow`] 才是可被
//! [`Runtime`] 执行的 Executable。数据连接的形状（整值、投影、组合、命名装配）由
//! [`Binding`] 描述。

use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use crate::core::binding::{Binding, BindingPlan, ReadMode, ResolveCtx, consume};
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
    /// 同一个 Binding 内对同一位置既消费又复用（或重复消费）。
    ///
    /// 消费读取会把整个位置的值移走，同一 Binding 内再读取同一个位置会产生隐式的求值顺序
    /// 依赖。[`then_move`](FlowBuilder::then_move)／[`output`](FlowBuilder::output) 的消费应
    /// 与复用读取分属不同步骤，或统一改用一种读取方式。
    ReadModeConflict,
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
            Self::ReadModeConflict => f.write_str(
                "同一个 Binding 内不能既消费又复用同一位置：请拆成不同步骤或统一读取方式",
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
/// [`then`](Self::then)（含 [`then_move`](Self::then_move)）的调用顺序就是执行顺序，与谁读取
/// 谁无关：即使某个 child 完全不使用前一步的 Output，它也不会被跳过、重排或并行。
///
/// # 读取方式
///
/// 同一个数据位置可以被多个 child 使用，但要显式选择读取方式（通过交给 [`then`](Self::then)
/// 的 Binding 表达）：
///
/// - 裸 [`Ref`]：复用读取。该位置之后仍可继续读取；本次执行按需要取副本，最后一次读取直接
///   拿走原值。要求该 Input 实现 [`Clone`]，因为复用意味着要复制出多份值。
/// - [`consume`]／[`then_move`](Self::then_move)：消费读取。把该位置的
///   值交给这一步，之后不能再读取它；不要求 [`Clone`]，用于非 `Clone` 值或一次性直通。
/// - [`field!`](crate::field)：字段投影。借用根、复制字段，根不必实现 `Clone`，该位置之后
///   仍可继续读取。
///
/// 位置是“先声明后使用”的：`Ref` 只能由 [`input`](Self::input) 与 [`then`](Self::then) 的
/// 返回值产生，因此不存在指向尚未声明位置的 `Ref`。
///
/// # 数据所有权策略与代价
///
/// 每个位置的值在产生时存入本次执行的值存储。读取方式决定它怎么被交给 child：
///
/// - 复用读取（裸 [`Ref`]）：只有当它是该位置的**最后一次读取**时才直接
///   移动原值，否则克隆整值；
/// - 消费读取（[`consume`]／[`then_move`](Self::then_move)／[`output`](Self::output)）：
///   把值移出，且必为该位置的最后一次读取；
/// - 字段投影（[`field!`](crate::field)）：借用根、只克隆目标字段，永远不移动根，也不消费
///   该位置。
///
/// 因此复制代价取决于**该位置所有读取的实际顺序**（顺序即 `then` 的声明顺序），不能只看整值
/// 读取次数：设某位置依次发生 r₁…rₙ 次读取，则整值克隆次数＝复用读取次数 − [rₙ 是复用读取]，
/// 投影读取额外各克隆一次字段。例如：
///
/// - 两个 child 用裸 `Ref` 复用、再由 `output` 取出：顺序 [复用, 复用, 消费]，前两次各克隆
///   整值、最后一次移动 → 整值复制 2 次；
/// - 先裸 `Ref` 复用一次、随后投影字段：顺序 [复用, 投影]，此时最后一次读取是投影，复用不是
///   最后一次 → **整值复制 1 次**（整值读取只有 1 次也照样复制）；
/// - 先投影、最后裸 `Ref` 复用一次：顺序 [投影, 复用]，复用是最后一次 → 直接移动，整值复制
///   0 次；
/// - 只投影一个字段：不复制根，只复制该字段。
///
/// 与其他候选方案的比较：
///
/// | 候选 | 结果 | 为什么不采用 |
/// | --- | --- | --- |
/// | 所有读取都 `Clone` | API 形式最简单 | 单消费者链路也要复制大对象（SES 的长文本代价明显） |
/// | 值以 `Arc<T>` 保存 | 读取只递增引用计数 | child 的 Input 是 owned 的 `T`，共享后仍要交出一个 `T`，无法避免复制；除非把 `Arc<T>` 泄漏进业务侧 Input |
/// | 借用（把 Input 改成 `&T`） | 零复制 | 会改变 T01 的 Input 契约，并把生命周期传播到每个 Node 与组合型 Executable |
/// | 全部按所有权转移 | 零复制 | 值被取走后第二个 child 无法使用，破坏 Ref 可复用的设计语义 |
/// | 为投影复制整个根 | 结构简单 | 取一个小字段却要复制根上的大字段，失去投影的意义 |
///
/// 这套策略保持 T01 的 owned Input 与 `Send` Future：`Flow` 与它的值存储仍然只保存
/// `Send` 的业务值；字段投影只借用根、复制字段，不把根复制进业务侧，类型擦除继续留在框架
/// 内部。
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
    /// 构建期登记的读取次数；整值复用、字段投影与消费读取都计入。执行期用于判断某次读取
    /// 是否为最后一次（只有最后一次复用/消费才把值移出存储）。
    reads: u32,
    /// 值是否已经被消费读取（`consume`／`then_move`／`output`）取走。
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

    /// 加入一个 child，并声明它的 Input 如何从当前 Flow 已有数据中形成（Binding）。
    ///
    /// `binding` 可以是：
    ///
    /// - 一个 [`Ref<T>`](Ref)：复用整值，要求 `T: Clone`，之后仍可继续读取该位置；
    /// - [`consume(ref)`](crate::core::consume)：显式消费整值，不要求 `Clone`，之后不能再读取；
    /// - [`field!`](crate::field) 的字段投影：借用根、复制字段，根不需要 `Clone`；
    /// - 1～8 元 tuple：元素是任意 Binding（可混用 `Ref`、`field!`、`consume`、`bind!`）；
    /// - [`bind!`](crate::bind) 的命名结构装配，并支持嵌套。
    ///
    /// Binding 的输出类型必须与 child 的 Input 一致，类型不一致无法编译。
    ///
    /// # 构建期校验（原子）
    ///
    /// Binding 依赖的每个根 `Ref` 都必须属于当前 Flow。外来 Ref 无论藏在字段投影、tuple 还是
    /// 嵌套装配中，都会在构建阶段被拒绝；失败时不登记 child，也不修改 builder：
    ///
    /// - [`FlowBuildError::ForeignRef`]：某个根 `Ref` 属于另一个 Flow。
    /// - [`FlowBuildError::SourceAlreadyConsumed`]：某个位置已被消费读取取走。
    /// - [`FlowBuildError::ReadModeConflict`]：同一 Binding 内对同一位置既消费又复用（或重复消费）。
    ///
    /// # 类型连接
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
    /// let input = flow.input();          // Ref<u32> 的 Output 是 u32
    /// // Length 的 Input 是 String，绑定输出不匹配，无法编译。
    /// let _ = flow.then(Length, input);
    /// ```
    ///
    /// 复用整值要求 `T: Clone`；非 `Clone` 值请用 [`consume`] 或
    /// [`then_move`](Self::then_move)。
    ///
    /// ```compile_fail
    /// use srflow::{ExecutionError, FlowBuilder, Node};
    ///
    /// struct Payload(String);            // 没有实现 Clone
    ///
    /// struct Describe;
    /// impl Node for Describe {
    ///     type Input = Payload;
    ///     type Output = usize;
    ///     async fn run(&self, input: Payload) -> Result<usize, ExecutionError> {
    ///         Ok(input.0.len())
    ///     }
    /// }
    ///
    /// let mut flow = FlowBuilder::<Payload>::new();
    /// let input = flow.input();
    /// // 裸 Ref<Payload> 要求 Payload: Clone；这里应该用 consume(input) 或 then_move。
    /// let _ = flow.then(Describe, input);
    /// ```
    ///
    /// # 示例：字段投影
    ///
    /// ```
    /// use srflow::{field, ExecutionError, FlowBuilder, Node, Runtime};
    ///
    /// // 根结构没有实现 Clone。
    /// struct Story { plan: String }
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
    /// let mut flow = FlowBuilder::<Story>::new();
    /// let story = flow.input();
    /// let length = flow.then(Length, field!(story.plan)).unwrap();
    /// let flow = flow.output(length).unwrap();
    ///
    /// let runtime = Runtime::new();
    /// let story = Story { plan: String::from("abcd") };
    /// let output = futures::executor::block_on(runtime.execute(&flow, story)).unwrap();
    /// assert_eq!(output, 4);
    /// ```
    pub fn then<E, B>(
        &mut self,
        executable: E,
        binding: B,
    ) -> Result<Ref<E::Output>, FlowBuildError>
    where
        E: Executable + Send + Sync + 'static,
        E::Input: Send + 'static,
        E::Output: 'static,
        B: Binding<Output = E::Input> + Send + Sync + 'static,
    {
        let plan = binding.__plan();
        self.commit_plan(&plan)?;
        let target = self.next_slot();
        self.steps
            .push(Box::new(BindingStep::new(executable, binding, target)));
        Ok(Ref::new(self.id, target))
    }

    /// 加入一个 child，并把该位置的值交给它（消费读取）。
    ///
    /// 等价于 `then(executable, consume(source))`，是 T02 用法的兼容便捷方法：读取语义、构建期
    /// 校验与读取登记都走同一套 [`then`](Self::then) 路径，不另建系统。不要求 Input 实现
    /// [`Clone`]，因此非 `Clone` 的业务值可以沿“Input → child → … → Output”直通。
    ///
    /// # 错误
    ///
    /// 与 [`then`](Self::then) 相同。
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
        self.then(executable, consume(source))
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

    /// 校验并一次性提交一个 Binding 的读取计划。
    ///
    /// 先对计划里所有根 Ref 做归属、位置有效性、既有消费状态与计划内部读/消费冲突的完整校验；
    /// 任一项失败时直接返回，`reads`、`consumed` 与 Step 列表都不变。全部通过后才写入读取计数，
    /// 避免逐个登记在第 k 个 Ref 失败时留下前 k−1 条读取记录。
    fn commit_plan(&mut self, plan: &BindingPlan) -> Result<(), FlowBuildError> {
        // 按 slot 聚合读取次数与消费次数；`Vec` 而非 `HashMap`，计划规模很小。
        let mut totals: Vec<(SlotId, u32, u32)> = Vec::new();
        for entry in &plan.0 {
            if entry.flow != self.id {
                return Err(FlowBuildError::ForeignRef);
            }
            let Some(meta) = self.slots.get(entry.slot.0) else {
                return Err(FlowBuildError::Invariant(InvariantError::new(
                    "绑定引用了本 Flow 之外的数据位置",
                )));
            };
            if meta.consumed {
                return Err(FlowBuildError::SourceAlreadyConsumed);
            }
            let consumes = u32::from(entry.mode == ReadMode::Consume);
            match totals.iter_mut().find(|(slot, _, _)| *slot == entry.slot) {
                Some((_, reads, consumes_total)) => {
                    *reads += 1;
                    *consumes_total += consumes;
                }
                None => totals.push((entry.slot, 1, consumes)),
            }
        }

        // 计划内部冲突：同一位置既消费又复用（或重复消费）。
        for (_, reads, consumes) in &totals {
            if *consumes > 0 && *reads > 1 {
                return Err(FlowBuildError::ReadModeConflict);
            }
        }

        // 计数溢出在写入前统一检查，保证提交阶段不会中途失败。
        for (slot, reads, _) in &totals {
            self.slots[slot.0]
                .reads
                .checked_add(*reads)
                .ok_or_else(|| {
                    FlowBuildError::Invariant(InvariantError::new("数据位置读取计数溢出"))
                })?;
        }

        for (slot, reads, consumes) in totals {
            let meta = &mut self.slots[slot.0];
            meta.reads += reads;
            if consumes > 0 {
                meta.consumed = true;
            }
        }
        Ok(())
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

/// 一步的执行：先解析 Binding 得到 owned Input，再经 Runtime 执行 child，最后写回 Output。
///
/// 不同类型、不同 Input／Output 的 child 由泛型 `E` 表示，Binding 由泛型 `B` 表示；两者的
/// 类型关系在 [`FlowBuilder::then`] 的签名处已经固定。Binding 解析发生在 child 启动之前，
/// 并且不经过 [`Runtime`]。
struct BindingStep<E, B> {
    executable: E,
    binding: B,
    target: SlotId,
}

impl<E, B> BindingStep<E, B> {
    fn new(executable: E, binding: B, target: SlotId) -> Self {
        Self {
            executable,
            binding,
            target,
        }
    }
}

impl<E, B> Step for BindingStep<E, B>
where
    E: Executable + Send + Sync + 'static,
    E::Input: Send + 'static,
    E::Output: 'static,
    B: Binding<Output = E::Input> + Send + Sync + 'static,
{
    fn run<'a>(&'a self, runtime: &'a Runtime, store: &'a mut ValueStore) -> StepFuture<'a> {
        Box::pin(async move {
            let input = self.binding.__resolve(&mut ResolveCtx(&mut *store))?;
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
    use crate::core::binding::PlanEntry;
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
        let step = BindingStep::new(
            Counting {
                runs: Arc::clone(&runs),
            },
            consume(Ref::new(FlowId::next(), SlotId(0))),
            SlotId(1),
        );
        // 位置 0 登记了一次消费读取，但从来没有值写入：这是框架不变量被破坏的情形。
        let mut store = ValueStore::new(&[1, 0]);

        let error = futures::executor::block_on(step.run(&Runtime::new(), &mut store)).unwrap_err();

        assert!(matches!(error, ExecutionError::Invariant(_)));
        assert_eq!(runs.load(Ordering::SeqCst), 0, "child 不得被执行");
    }

    /// 定向验证：计划中后位出现外来 Ref 时，前位读取也不得被登记。
    ///
    /// 外部测试只能观察到“结果仍正确”，而多登记一次读取只会让该次读取走克隆路径、结果不变；
    /// 这里直接断言登记状态，才能真正证明失败是原子的。
    #[test]
    fn a_foreign_entry_in_the_middle_registers_nothing() {
        let mut flow = FlowBuilder::<String>::new();
        let input = flow.input();
        let plan = BindingPlan(vec![
            PlanEntry {
                flow: flow.id,
                slot: input.slot(),
                mode: ReadMode::Read,
            },
            PlanEntry {
                flow: FlowId::next(),
                slot: input.slot(),
                mode: ReadMode::Read,
            },
        ]);

        assert_eq!(
            flow.commit_plan(&plan).unwrap_err(),
            FlowBuildError::ForeignRef
        );
        assert_eq!(
            flow.slots[INPUT_SLOT.0].reads, 0,
            "失败不得留下前一条读取登记"
        );
        assert!(!flow.slots[INPUT_SLOT.0].consumed);
        assert!(flow.steps.is_empty());
    }

    /// 定向验证：计划内部冲突时，`reads`、`consumed` 与 Step 列表都不变。
    #[test]
    fn a_conflicting_entry_registers_nothing() {
        let mut flow = FlowBuilder::<String>::new();
        let input = flow.input();
        let plan = BindingPlan(vec![
            PlanEntry {
                flow: flow.id,
                slot: input.slot(),
                mode: ReadMode::Consume,
            },
            PlanEntry {
                flow: flow.id,
                slot: input.slot(),
                mode: ReadMode::Read,
            },
        ]);

        assert_eq!(
            flow.commit_plan(&plan).unwrap_err(),
            FlowBuildError::ReadModeConflict
        );
        assert_eq!(flow.slots[INPUT_SLOT.0].reads, 0, "失败不得登记任何读取");
        assert!(!flow.slots[INPUT_SLOT.0].consumed, "失败不得标记消费");
        assert!(flow.steps.is_empty());
    }
}
