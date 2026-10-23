//! Node 声明协议与类型化调用桥（SPEC §5、Engine §5）。
//!
//! [`Node`] 是结构体形式的统一协议；函数形式的自然异步签名通过 [`Callable`] 的函数
//! 实现接入。调用桥在具体 `N`／`Input`／`Output` 类型仍已知时构造 Query、await 具体
//! Future 并返回类型化输出；它不创建 Root／Guard，也不替 Engine 猜业务类型。

use crate::core::engine::data::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::flow::Flow;
use crate::core::engine::shape::{
    InputOps, InputSpec, NodeOutput, OutputOps, Query, Ref, RefShape, RootInput, ShapeOps,
};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

/// 异步业务叶子：只接收声明输入的只读 Query，返回新的 owned Data 或 `()`。
///
/// 不接触 Ref、Context、Scope 或 Container，也不启动 SRFlow child。
///
/// `run` 使用 `async fn` 表达冻结契约；返回的 Future 不承诺 `Send`（本版允许非 Send
/// Data／Future），因此这里明确不附加 auto-trait 约束。
#[allow(async_fn_in_trait)]
pub trait Node {
    /// 业务输入声明：`()`、单 Data 或 2～16 个 flat Data tuple。
    type Input: InputSpec;
    /// 业务输出声明：单个 owned Data 或 `()`。
    type Output: NodeOutput;

    /// 执行契约：`Query<&Self::Input>` 的视图是各叶子不可变借用。
    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError>;
}

impl<N: Node + ?Sized> Node for &N {
    type Input = N::Input;
    type Output = N::Output;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        (**self).run(query).await
    }
}

impl<N: Node> Node for Arc<N> {
    type Input = N::Input;
    type Output = N::Output;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        (**self).run(query).await
    }
}

/// 函数形式 Node 的接线标记，携带函数声明的输出类型。
#[doc(hidden)]
pub struct FunctionCall<O>(pub(crate) PhantomData<fn() -> O>);

/// 结构体／`&Node`／`Arc<Node>` 形式的接线标记。
#[doc(hidden)]
pub struct StructCall;

/// 登记与调用协议：把 Ref 形状 `A` 与具体 Node 调用形式 `K` 连接到输出声明。
///
/// 只有类型化调用桥会调用 [`Callable::call`]；业务侧只通过 `Flow::then` 使用它，
/// 不给调用者提供伪造 callable 的入口。
pub trait Callable<A: RefShape, K: 'static> {
    /// Node 的输出声明类型。
    type Output: NodeOutput;

    /// 构造一次真实调用：Query 借用限定在本 Future 内，输出类型保持具体。
    #[doc(hidden)]
    fn call<'a>(
        &'a self,
        query: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output, BodyError>> + 'a>>;
}

impl<F, A, O> Callable<A, FunctionCall<O>> for F
where
    A: RefShape,
    O: NodeOutput,
    F: for<'a> AsyncFn(Query<<A::Spec as InputSpec>::Borrowed<'a>>) -> Result<O, BodyError>,
{
    type Output = O;

    fn call<'a>(
        &'a self,
        query: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<O, BodyError>> + 'a>> {
        Box::pin(async move { self(query).await })
    }
}

impl<N, A> Callable<A, StructCall> for N
where
    N: Node,
    A: RefShape<Spec = N::Input>,
{
    type Output = N::Output;

    fn call<'a>(
        &'a self,
        query: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output, BodyError>> + 'a>> {
        Box::pin(async move {
            // 视图类型与 `Query<&Self::Input>` 的视图相同；这里只做类型层面的重建。
            self.run(Query::<&N::Input>::from_view(query.get())).await
        })
    }
}

// ---------------------------------------------------------------- 内部自动推导桥

/// 自动推导桥的函数形式 marker：把具体 Query 借用形状与输出类型编码进类型参数，
/// 使 per-arity impl 之间不重叠，并让调用侧的输入 Shape 由 impl 唯一确定。
///
/// 隐藏支持项：业务**不构造、不指定**它；它只作为 [`AutoCallable`] 的 marker 参与
/// [`Flow::then`]／[`Flow::then_output`] 的公共推导。构造器与字段保持私有，无法从外部伪造。
#[derive(Debug)]
#[doc(hidden)]
pub struct AutoFunction<I, O>(PhantomData<fn(I) -> O>);

/// 自动推导桥：由具体 callable 签名在编译期确定输入 RefShape、输出与登记 marker。
///
/// 隐藏支持协议：业务**不实现、不命名**它，也不手填 marker 或输入泛型；它只出现在
/// [`Flow::then`]／[`Flow::then_output`] 的 bound 中，由函数/结构体签名自然推导。
/// 函数形式按具体 `Query<...>` 借用形状逐个 arity 匹配，不依赖
/// `<A::Spec as InputSpec>::Borrowed` 投影反推。`register` 把已解析的显式 RefShape 交给
/// 唯一登记实现 [`Flow::then_with_output`]。
#[doc(hidden)]
#[allow(private_bounds)]
pub trait AutoCallable<K: 'static> {
    /// 推导出的输入 RefShape；槽类型与顺序来自具体签名。
    type Input: RefShape + ShapeOps;
    /// Node 输出声明。
    type Output: NodeOutput;

    /// 用已解析的输入 Shape 登记该 Node Step。
    fn register<'n>(
        self,
        flow: &mut Flow<'n>,
        input: Self::Input,
    ) -> <Self::Output as NodeOutput>::Refs
    where
        Self: 'n;
}

impl<F, O> AutoCallable<AutoFunction<(), O>> for F
where
    F: for<'a> AsyncFn(Query<()>) -> Result<O, BodyError>,
    O: NodeOutput + OutputOps,
    <O as NodeOutput>::Refs: ShapeOps,
{
    type Input = ();
    type Output = O;

    fn register<'n>(self, flow: &mut Flow<'n>, input: ()) -> <O as NodeOutput>::Refs
    where
        Self: 'n,
    {
        flow.then_with_output::<Self, (), FunctionCall<O>>(self, input)
    }
}

/// 为每个承诺的 arity 生成函数形式桥：键是具体 `Query<...>` 借用形状与 RefShape 槽类型。
macro_rules! auto_function_arities {
    ($spec:ty, $refs:ty, $borrow:ty; $($t:ident),+) => {
        impl<Func, Out, $($t: Data),+> AutoCallable<AutoFunction<$spec, Out>> for Func
        where
            Func: for<'a> AsyncFn(Query<$borrow>) -> Result<Out, BodyError>,
            Out: NodeOutput + OutputOps,
            <Out as NodeOutput>::Refs: ShapeOps,
        {
            type Input = $refs;
            type Output = Out;

            fn register<'n>(self, flow: &mut Flow<'n>, input: Self::Input) -> <Out as NodeOutput>::Refs
            where
                Self: 'n,
            {
                flow.then_with_output::<Self, Self::Input, FunctionCall<Out>>(self, input)
            }
        }
    };
}

auto_function_arities!((A,), Ref<A>, &'a A; A);
auto_function_arities!((A, B), (Ref<A>, Ref<B>), (&'a A, &'a B); A, B);
auto_function_arities!((A, B, C), (Ref<A>, Ref<B>, Ref<C>), (&'a A, &'a B, &'a C); A, B, C);
auto_function_arities!(
    (A, B, C, D), (Ref<A>, Ref<B>, Ref<C>, Ref<D>), (&'a A, &'a B, &'a C, &'a D); A, B, C, D
);
auto_function_arities!(
    (A, B, C, D, E), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>),
    (&'a A, &'a B, &'a C, &'a D, &'a E); A, B, C, D, E
);
auto_function_arities!(
    (A, B, C, D, E, F), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F); A, B, C, D, E, F
);
auto_function_arities!(
    (A, B, C, D, E, F, G), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G); A, B, C, D, E, F, G
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H); A, B, C, D, E, F, G, H
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I); A, B, C, D, E, F, G, H, I
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J);
    A, B, C, D, E, F, G, H, I, J
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J, K),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K);
    A, B, C, D, E, F, G, H, I, J, K
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J, K, L),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L);
    A, B, C, D, E, F, G, H, I, J, K, L
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J, K, L, M),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M);
    A, B, C, D, E, F, G, H, I, J, K, L, M
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>, Ref<N>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M, &'a N);
    A, B, C, D, E, F, G, H, I, J, K, L, M, N
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>, Ref<N>, Ref<O>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M, &'a N, &'a O);
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O
);
auto_function_arities!(
    (A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P),
    (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>, Ref<N>, Ref<O>, Ref<P>),
    (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M, &'a N, &'a O, &'a P);
    A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P
);

/// 结构体／`&Node`／`Arc<Node>` 形式的自动桥：复用既有 `RootInput::Refs` 映射。
///
/// 只在内部自动入口加约束，不收紧公开 `Node::Input: InputSpec`。
impl<N> AutoCallable<StructCall> for N
where
    N: Node,
    N::Input: RootInput + InputOps,
    <N::Input as RootInput>::Refs: ShapeOps,
    N::Output: OutputOps,
    <N::Output as NodeOutput>::Refs: ShapeOps,
{
    type Input = <N::Input as RootInput>::Refs;
    type Output = N::Output;

    fn register<'n>(
        self,
        flow: &mut Flow<'n>,
        input: Self::Input,
    ) -> <N::Output as NodeOutput>::Refs
    where
        Self: 'n,
    {
        flow.then_with_output::<Self, Self::Input, StructCall>(self, input)
    }
}

// ---------------------------------------------------------------- 同步 Factory 包装

/// 同步 Factory 的零输入 Node 包装：`run` 才调用 Factory，每次返回新的 owned Data。
///
/// Definition 只保存 Factory 能力；不缓存返回值、不隐式 Clone、不要求 `'static`／Send。
#[allow(dead_code)] // T03 公开前由内部测试/后续任务使用
pub(crate) struct ProvideNode<F, T> {
    factory: F,
    marker: PhantomData<fn() -> T>,
}

impl<F, T> ProvideNode<F, T> {
    /// 包装一个同步 Factory。
    pub(crate) fn new(factory: F) -> Self {
        Self {
            factory,
            marker: PhantomData,
        }
    }
}

impl<F, T> Node for ProvideNode<F, T>
where
    F: Fn() -> T,
    T: Data,
{
    type Input = ();
    type Output = T;

    async fn run(&self, _query: Query<&()>) -> Result<T, BodyError> {
        Ok((self.factory)())
    }
}
