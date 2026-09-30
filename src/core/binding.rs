//! Binding：把当前 Flow 已有数据**结构性**装配成 child 的 Input。
//!
//! Binding 是数据连接机制，不是执行单元：它在 child 启动之前解析，只读取、投影、组合当前
//! Flow 已经声明的值，不进入 [`Runtime`](crate::core::Runtime)，也不产生新的业务意义。
//! 业务判断与计算属于 [`Node`](crate::core::Node)。
//!
//! # 可用形状
//!
//! - 整值读取：[`Ref<T>`](crate::core::Ref) 本身就是一种 Binding，复用该位置的值。
//! - 显式消费：[`consume`] 把整值移动给下游，不要求 `Clone`。
//! - 字段投影：[`field!`](crate::field) 从结构化数据中取出字段，不消费根值。
//! - 多值组合：2～8 元的 tuple，元素可以是任意 Binding 的组合。
//! - 命名结构装配：[`bind!`](crate::bind) 按字段名构造下游业务结构。
//! - 嵌套装配：已构造的 Binding 可以再作为更大 Binding 的一部分。
//!
//! # 结构性边界
//!
//! [`Binding`] 是封闭 trait：只有框架定义的结构类型实现它，业务侧无法为自定义类型实现，也
//! 没有 `map(any_function)` 之类的任意函数入口。字段投影与结构装配经由 [`field!`](crate::field)、
//! [`bind!`](crate::bind) 两个宏表达。
//!
//! 但为了让这两个宏能在业务 crate 中展开，框架必须保留两个 `#[doc(hidden)]` 的公开构造入口
//! [`__project_field`]、[`__assemble`]，它们**不是封闭的**：
//!
//! - 直接调用 [`__project_field`] 可以注入根外数据（其回调可返回 `&'static`）；
//! - 直接调用 [`__assemble`] 可以在回调里写任意计算。
//!
//! 这是本阶段已知的剩余漏洞，由文档与评审承担（宏参数里写出满足约束的表达式同样在此列）。
//! 详见 crate 首页的“Binding 的结构性边界”。
//!
//! # Flow 归属
//!
//! Binding 依赖的每个根 [`Ref`] 都必须属于当前 Flow。外来 Ref 无论藏在
//! 字段投影、tuple 还是嵌套装配中，都在**构建阶段**以 [`FlowBuildError`] 拒绝，并且拒绝时
//! 不修改 builder：读取计划整体校验通过后才一次性提交。
//!
//! [`FlowBuildError`]: crate::core::FlowBuildError

use std::marker::PhantomData;

use crate::core::ExecutionError;
use crate::core::reference::{FlowId, Ref, SlotId};
use crate::core::value_store::ValueStore;

/// 一次读取的消耗方式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadMode {
    /// 非消费读取：整值复用或字段投影，值在读取后仍留在存储中。
    Read,
    /// 消费读取：把整值移动给下游，该位置之后不能再读取。
    Consume,
}

/// 读取计划中的一条记录。
#[derive(Debug, Clone, Copy)]
pub(crate) struct PlanEntry {
    pub(crate) flow: FlowId,
    pub(crate) slot: SlotId,
    pub(crate) mode: ReadMode,
}

/// Binding 在构建阶段登记的读取计划。
///
/// 这是框架内部类型，只为封闭 [`Binding`] trait 的签名而公开：它没有公开字段或方法，业务侧
/// 无法构造或修改。读取计划在 `then` 中整体校验后才提交，避免部分登记。
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct BindingPlan(pub(crate) Vec<PlanEntry>);

impl BindingPlan {
    pub(crate) fn push(&mut self, flow: FlowId, slot: SlotId, mode: ReadMode) {
        self.0.push(PlanEntry { flow, slot, mode });
    }

    pub(crate) fn merge(&mut self, other: BindingPlan) {
        self.0.extend(other.0);
    }
}

/// Binding 解析期访问本次执行值存储的上下文。
///
/// 框架内部类型，只为封闭 [`Binding`] trait 的签名而公开：没有公开字段或方法，业务侧无法
/// 构造或使用它。
#[doc(hidden)]
pub struct ResolveCtx<'a>(pub(crate) &'a mut ValueStore);

impl ResolveCtx<'_> {
    pub(crate) fn read_reuse<T>(&mut self, slot: SlotId) -> Result<T, ExecutionError>
    where
        T: Send + Clone + 'static,
    {
        self.0.read_shared::<T>(slot)
    }

    pub(crate) fn read_consume<T>(&mut self, slot: SlotId) -> Result<T, ExecutionError>
    where
        T: Send + 'static,
    {
        self.0.read_consuming::<T>(slot)
    }

    pub(crate) fn read_project<Root, F>(
        &mut self,
        slot: SlotId,
        project: fn(&Root) -> &F,
    ) -> Result<F, ExecutionError>
    where
        Root: Send + 'static,
        F: Clone + Send + 'static,
    {
        self.0.read_projected::<Root, F>(slot, project)
    }
}

mod private {
    /// 封闭 [`super::Binding`]：业务侧无法命名本 trait，因此无法实现 `Binding`。
    pub trait Sealed {}
}

/// 数据连接描述：说明一个 child 的 Input 如何从当前 Flow 已有数据中结构性形成。
///
/// 普通使用者不实现本 trait，也不需要手写它的实现：直接把
/// [`Ref`]、[`consume`] 的结果、[`field!`](crate::field) 的投影、tuple 或 [`bind!`](crate::bind)
/// 的装配结果交给 [`FlowBuilder::then`](crate::core::FlowBuilder::then) 即可。
///
/// # 封闭性
///
/// 本 trait 是 sealed 的：它继承一个不可命名的私有 supertrait，因此只有框架定义的结构类型
/// 能实现它。这保证业务侧不能通过实现 `Binding` 把任意计算塞进数据连接。
///
/// # 关联类型
///
/// [`Output`](Binding::Output) 是解析后形成的下游 Input 类型；`then` 会要求它与目标
/// Executable 的 `Input` 一致，不一致由编译器拒绝。
pub trait Binding: private::Sealed {
    /// 解析后形成的下游 Input 类型。
    type Output;

    /// 构建期：登记本 Binding 依赖的所有根 Ref 与读取方式（框架内部）。
    #[doc(hidden)]
    fn __plan(&self) -> BindingPlan;

    /// 执行期：在 child 启动之前，从值存储解析出 owned Input（框架内部）。
    #[doc(hidden)]
    fn __resolve(&self, ctx: &mut ResolveCtx<'_>) -> Result<Self::Output, ExecutionError>;
}

impl<T> private::Sealed for Ref<T> {}

/// 整值复用读取：位置的值被复制给下游，该位置之后仍可继续读取。
///
/// 这是 T02 已有语义的延续，要求 `T: Clone`。非 `Clone` 值请用 [`consume`]。
impl<T> Binding for Ref<T>
where
    T: Clone + Send + 'static,
{
    type Output = T;

    fn __plan(&self) -> BindingPlan {
        let mut plan = BindingPlan::default();
        plan.push(self.flow(), self.slot(), ReadMode::Read);
        plan
    }

    fn __resolve(&self, ctx: &mut ResolveCtx<'_>) -> Result<T, ExecutionError> {
        ctx.read_reuse::<T>(self.slot())
    }
}

/// 显式消费读取的 Binding：把某个位置的值移动给下游。
///
/// 由 [`consume`] 构造。它不要求 `T: Clone`，用于非 `Clone` 值的一次性直通；一旦消费，该
/// 位置之后不能再被任何 Binding 或 [`output`](crate::core::FlowBuilder::output) 读取。
#[derive(Debug)]
pub struct Consume<T> {
    reference: Ref<T>,
    _marker: PhantomData<fn() -> T>,
}

/// 把 `reference` 转换为一次显式消费读取。
///
/// # 示例：非 `Clone` 值直通
///
/// ```
/// use srflow::{consume, ExecutionError, FlowBuilder, Node, Runtime};
///
/// struct Payload(String); // 没有实现 Clone
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
/// let length = flow.then(Length, consume(input)).unwrap();
/// let flow = flow.output(length).unwrap();
///
/// let runtime = Runtime::new();
/// let output =
///     futures::executor::block_on(runtime.execute(&flow, Payload(String::from("abcd")))).unwrap();
/// assert_eq!(output, 4);
/// ```
pub fn consume<T>(reference: Ref<T>) -> Consume<T> {
    Consume {
        reference,
        _marker: PhantomData,
    }
}

impl<T> private::Sealed for Consume<T> {}

impl<T> Binding for Consume<T>
where
    T: Send + 'static,
{
    type Output = T;

    fn __plan(&self) -> BindingPlan {
        let mut plan = BindingPlan::default();
        plan.push(
            self.reference.flow(),
            self.reference.slot(),
            ReadMode::Consume,
        );
        plan
    }

    fn __resolve(&self, ctx: &mut ResolveCtx<'_>) -> Result<T, ExecutionError> {
        ctx.read_consume::<T>(self.reference.slot())
    }
}

/// 从结构化 Flow 数据中投影一个字段的 Binding。
///
/// 由 [`field!`](crate::field) 宏构造。投影**不消费根值**：它在 child 启动前借用根、复制目标
/// 字段，因此根本身不需要实现 `Clone`，但被投影的字段需要（`Copy` 也满足 `Clone`）。
///
/// 通过 [`field!`](crate::field) 使用时，投影只读取已有结构，不产生新的业务判断或计算。注意
/// 框架内部的 [`__project_field`] 构造入口本身并不封闭：直接调用它可以注入根外数据（见其
/// 文档与 crate 首页的“Binding 的结构性边界”）。
#[derive(Debug)]
pub struct Field<Root, F> {
    reference: Ref<Root>,
    project: fn(&Root) -> &F,
    _marker: PhantomData<fn() -> F>,
}

impl<Root, F> private::Sealed for Field<Root, F> {}

impl<Root, F> Binding for Field<Root, F>
where
    Root: Send + 'static,
    F: Clone + Send + 'static,
{
    type Output = F;

    fn __plan(&self) -> BindingPlan {
        let mut plan = BindingPlan::default();
        plan.push(self.reference.flow(), self.reference.slot(), ReadMode::Read);
        plan
    }

    fn __resolve(&self, ctx: &mut ResolveCtx<'_>) -> Result<F, ExecutionError> {
        ctx.read_project::<Root, F>(self.reference.slot(), self.project)
    }
}

/// 构造一次字段投影（框架内部，由 [`field!`](crate::field) 宏调用）。
///
/// # 这不是一个封闭入口
///
/// `project` 的签名是 `fn(&Root) -> &F`。Rust 无法要求返回值**来自** `&Root`：一个忽略根、返回
/// `&'static F`（或任何活得足够久的外部引用）的函数同样满足该签名。直接调用本入口，下游就会
/// 收到当前 Flow 中原本不存在的数据——这是本阶段已知的**根外数据注入**漏洞，类型系统没有
/// 封死它（例如：`__project_field(root, |_| &STATIC_VALUE)`）。
///
/// 因此业务侧不要直接调用本函数，请使用 [`field!`](crate::field)；`field!` 只会生成
/// `&根.字段路径`，不会注入外部数据。边界说明见 crate 首页的“Binding 的结构性边界”。
#[doc(hidden)]
pub fn __project_field<Root, F>(reference: Ref<Root>, project: fn(&Root) -> &F) -> Field<Root, F> {
    Field {
        reference,
        project,
        _marker: PhantomData,
    }
}

/// 从结构化 Flow 数据中投影字段。
///
/// 用法为 `field!(根 Ref 名 . 字段路径)`，例如 `field!(story.plan)` 或
/// `field!(story.meta.author)`。宏只接受**裸标识符**作为根（`$root:ident`），因为
/// `macro_rules!` 的 `expr` 片段后面不能直接跟 `.`。
///
/// # 示例
///
/// ```
/// use srflow::{field, ExecutionError, FlowBuilder, Node, Runtime};
///
/// struct Story {
///     title: String,
///     // 一个大字段：投影 title 时不应复制它。
///     body: String,
/// }
///
/// struct TitleLength;
/// impl Node for TitleLength {
///     type Input = String;
///     type Output = usize;
///     async fn run(&self, input: String) -> Result<usize, ExecutionError> {
///         Ok(input.chars().count())
///     }
/// }
///
/// let mut flow = FlowBuilder::<Story>::new();
/// let story = flow.input();
/// let length = flow.then(TitleLength, field!(story.title)).unwrap();
/// let flow = flow.output(length).unwrap();
///
/// let runtime = Runtime::new();
/// let story = Story {
///     title: String::from("abcd"),
///     body: String::from("much longer body"),
/// };
/// let output = futures::executor::block_on(runtime.execute(&flow, story)).unwrap();
/// assert_eq!(output, 4);
/// ```
///
/// 字段类型必须与下游 Input 一致；投影只读取已有结构，不能产生新的业务值：
///
/// ```compile_fail
/// use srflow::{field, ExecutionError, FlowBuilder, Node};
///
/// struct Story { number: u32 }
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
/// // `field!(story.number)` 的 Output 是 u32，而 Length 需要 String，无法编译。
/// let _ = flow.then(Length, field!(story.number));
/// ```
#[macro_export]
macro_rules! field {
    ($root:ident . $($field:ident).+) => {
        $crate::__project_field($root, |__root| &__root.$($field).+)
    };
}

/// 按字段名装配下游业务结构的 Binding。
///
/// 由 [`bind!`](crate::bind) 宏构造。它把若干 Binding 的解析结果放进一个具名结构的字段中，是纯结构装配，
/// 不承担业务计算。
#[derive(Debug)]
pub struct Assemble<I, F, B> {
    construct: F,
    fields: B,
    _marker: PhantomData<fn() -> I>,
}

impl<I, F, B> private::Sealed for Assemble<I, F, B> {}

impl<I, F, B> Binding for Assemble<I, F, B>
where
    B: Binding,
    F: Fn(B::Output) -> I,
{
    type Output = I;

    fn __plan(&self) -> BindingPlan {
        self.fields.__plan()
    }

    fn __resolve(&self, ctx: &mut ResolveCtx<'_>) -> Result<I, ExecutionError> {
        let values = self.fields.__resolve(ctx)?;
        Ok((self.construct)(values))
    }
}

/// 构造一次命名结构装配（框架内部，由 [`bind!`](crate::bind) 宏调用）。
///
/// `construct` 只负责把已解析的字段值放进结构，不读取 Flow 存储；业务侧请使用 [`bind!`](crate::bind)。
#[doc(hidden)]
pub fn __assemble<I, F, B>(construct: F, fields: B) -> Assemble<I, F, B>
where
    B: Binding,
    F: Fn(B::Output) -> I,
{
    Assemble {
        construct,
        fields,
        _marker: PhantomData,
    }
}

/// 按字段名把若干 Binding 装配成下游业务结构。
///
/// 每个字段的值是一个 Binding（可以是 [`Ref`]、[`field!`](crate::field)、[`consume`]、
/// tuple、[`bind!`](crate::bind) 的结果，从而支持嵌套装配）。字段名只用于构造结构；数据来源由每个字段的
/// Binding 显式给出。
///
/// 至少支持 1～8 个字段；更多字段本阶段不要求。
///
/// # 示例
///
/// ```
/// use srflow::{bind, field, ExecutionError, FlowBuilder, Node, Runtime};
///
/// // 根结构没有实现 `Clone`：字段投影仍然可以读取它。
/// struct Story {
///     plan: String,
///     background: String,
/// }
///
/// struct MakeKey;
/// impl Node for MakeKey {
///     type Input = String;
///     type Output = u32;
///     async fn run(&self, input: String) -> Result<u32, ExecutionError> {
///         Ok(input.len() as u32)
///     }
/// }
///
/// struct ProgressionInput {
///     plan: String,
///     key: u32,
///     background: String,
/// }
///
/// struct Progression;
/// impl Node for Progression {
///     type Input = ProgressionInput;
///     type Output = String;
///     async fn run(&self, input: ProgressionInput) -> Result<String, ExecutionError> {
///         Ok(format!("{}-{}-{}", input.plan, input.key, input.background))
///     }
/// }
///
/// let mut flow = FlowBuilder::<Story>::new();
/// let story = flow.input();
/// let key = flow.then(MakeKey, field!(story.plan)).unwrap();
/// let input = bind!(ProgressionInput {
///     plan: field!(story.plan),
///     key: key,
///     background: field!(story.background),
/// });
/// let result = flow.then(Progression, input).unwrap();
/// let flow = flow.output(result).unwrap();
///
/// let runtime = Runtime::new();
/// let story = Story {
///     plan: String::from("plan"),
///     background: String::from("bg"),
/// };
/// let output = futures::executor::block_on(runtime.execute(&flow, story)).unwrap();
/// assert_eq!(output, "plan-4-bg");
/// ```
///
/// 字段类型必须与结构字段一致，任意计算结果不能充当 Binding：
///
/// ```compile_fail
/// use srflow::{bind, field, ExecutionError, FlowBuilder, Node};
///
/// struct Story { text: String }
/// struct Pair { a: u32, b: u32 }
///
/// struct Sum;
/// impl Node for Sum {
///     type Input = Pair;
///     type Output = u32;
///     async fn run(&self, input: Pair) -> Result<u32, ExecutionError> {
///         Ok(input.a + input.b)
///     }
/// }
///
/// let mut flow = FlowBuilder::<Story>::new();
/// let story = flow.input();
/// // `field!(story.text)` 是 String，而 Pair.a 是 u32，无法编译。
/// let _ = flow.then(Sum, bind!(Pair { a: field!(story.text), b: field!(story.text) }));
/// ```
///
/// ```compile_fail
/// use srflow::{bind, ExecutionError, FlowBuilder, Node};
///
/// struct In { a: String }
///
/// struct One;
/// impl Node for One {
///     type Input = In;
///     type Output = String;
///     async fn run(&self, input: In) -> Result<String, ExecutionError> {
///         Ok(input.a)
///     }
/// }
///
/// fn compute() -> String { String::from("computed") }
///
/// let mut flow = FlowBuilder::<String>::new();
/// let _input = flow.input();
/// // 任意计算结果不是 Binding，无法编译。
/// let _ = flow.then(One, bind!(In { a: compute() }));
/// ```
#[macro_export]
macro_rules! bind {
    ($ty:path { $($field:ident : $binding:expr),* $(,)? }) => {
        $crate::__assemble(
            |__fields| { let ($($field,)*) = __fields; $ty { $($field),* } },
            ($($binding,)*),
        )
    };
}

macro_rules! tuple_binding {
    ($($name:ident $value:ident),+) => {
        impl<$($name: Binding),+> private::Sealed for ($($name,)+) {}

        impl<$($name: Binding),+> Binding for ($($name,)+) {
            type Output = ($($name::Output,)+);

            fn __plan(&self) -> BindingPlan {
                let ($($value,)+) = self;
                let mut plan = BindingPlan::default();
                $( plan.merge($value.__plan()); )+
                plan
            }

            fn __resolve(&self, ctx: &mut ResolveCtx<'_>) -> Result<Self::Output, ExecutionError> {
                let ($($value,)+) = self;
                Ok(($($value.__resolve(ctx)?,)+))
            }
        }
    };
}

// tuple Binding：本阶段支持 1～8 元。更高元数不要求，也不视为设计语义。
tuple_binding!(A1 a1);
tuple_binding!(A1 a1, A2 a2);
tuple_binding!(A1 a1, A2 a2, A3 a3);
tuple_binding!(A1 a1, A2 a2, A3 a3, A4 a4);
tuple_binding!(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5);
tuple_binding!(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6);
tuple_binding!(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7);
tuple_binding!(A1 a1, A2 a2, A3 a3, A4 a4, A5 a5, A6 a6, A7 a7, A8 a8);
