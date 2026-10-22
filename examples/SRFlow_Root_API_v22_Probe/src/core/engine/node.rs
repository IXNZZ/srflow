//! Node 声明协议与类型化调用桥（SPEC §5、Engine §5）。
//!
//! [`Node`] 是结构体形式的统一协议；函数形式的自然异步签名通过 [`Callable`] 的函数
//! 实现接入。调用桥在具体 `N`／`Input`／`Output` 类型仍已知时构造 Query、await 具体
//! Future 并返回类型化输出；它不创建 Root／Guard，也不替 Engine 猜业务类型。

use crate::core::engine::error::BodyError;
use crate::core::engine::shape::{InputSpec, NodeOutput, Query, RefShape};
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
