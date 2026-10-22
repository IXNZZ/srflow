//! Root-only supplement over the existing Builder, Runtime and Data Core.
#![allow(missing_docs, private_bounds)]
use super::definition::{Builder, Definition};
use super::error::{BuildError, BuildErrorDetail};
use super::identity::BodyId;
use super::node::{Callable, FunctionCall, StructCall};
use super::shape::{InputOps, OutputOps, PositionDesc, ShapeOps, TypeDesc};
use crate::{BodyError, Data, Node, NodeOutput, Query, Ref, RefShape, RootInput};
use std::{cell::RefCell, marker::PhantomData, rc::Rc};
/// Reuses the existing trait bridge. The marker exposes concrete borrowed signatures
/// to inference; the original Callable projection alone does not infer the input shape.
#[doc(hidden)]
pub trait AutoCallable<K> {
    type Input: RefShape;
    type Output: NodeOutput;
    fn register<'n>(
        self,
        flow: &mut super::flow::Flow<'n>,
        input: Self::Input,
    ) -> <Self::Output as NodeOutput>::Refs
    where
        Self: 'n;
}
#[doc(hidden)]
pub struct AutoFunction<I, O>(PhantomData<fn(I) -> O>);
impl<F, O> AutoCallable<AutoFunction<(), O>> for F
where
    F: for<'a> AsyncFn(Query<()>) -> Result<O, BodyError>,
    O: NodeOutput + OutputOps,
    O::Refs: ShapeOps,
{
    type Input = ();
    type Output = O;
    fn register<'n>(self, flow: &mut super::flow::Flow<'n>, input: ()) -> O::Refs
    where
        Self: 'n,
    {
        flow.then::<_, (), FunctionCall<O>>(self, input)
    }
}
macro_rules! auto_function {
    ($spec:ty, $refs:ty, $borrow:ty; $($t:ident),+) => {
        impl<Func, Out, $($t: Data),+> AutoCallable<AutoFunction<$spec, Out>> for Func
        where Func: for<'a> AsyncFn(Query<$borrow>) -> Result<Out, BodyError>,
              Out: NodeOutput + OutputOps, Out::Refs: ShapeOps {
            type Input = $refs; type Output = Out;
            fn register<'n>(self, flow: &mut super::flow::Flow<'n>, input: Self::Input) -> Out::Refs where Self:'n {
                flow.then::<_, Self::Input, FunctionCall<Out>>(self, input)
            }
        }
    };
}
auto_function!((A,), Ref<A>, &'a A; A);
auto_function!((A, B), (Ref<A>, Ref<B>), (&'a A, &'a B); A, B);
auto_function!((A, B, C), (Ref<A>, Ref<B>, Ref<C>), (&'a A, &'a B, &'a C); A, B, C);
auto_function!((A, B, C, D), (Ref<A>, Ref<B>, Ref<C>, Ref<D>), (&'a A, &'a B, &'a C, &'a D); A, B, C, D);
auto_function!((A, B, C, D, E), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>), (&'a A, &'a B, &'a C, &'a D, &'a E); A, B, C, D, E);
auto_function!((A, B, C, D, E, F), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F); A, B, C, D, E, F);
auto_function!((A, B, C, D, E, F, G), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G); A, B, C, D, E, F, G);
auto_function!((A, B, C, D, E, F, G, H), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H); A, B, C, D, E, F, G, H);
auto_function!((A, B, C, D, E, F, G, H, I), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I); A, B, C, D, E, F, G, H, I);
auto_function!((A, B, C, D, E, F, G, H, I, J), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J); A, B, C, D, E, F, G, H, I, J);
auto_function!((A, B, C, D, E, F, G, H, I, J, K), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K); A, B, C, D, E, F, G, H, I, J, K);
auto_function!((A, B, C, D, E, F, G, H, I, J, K, L), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L); A, B, C, D, E, F, G, H, I, J, K, L);
auto_function!((A, B, C, D, E, F, G, H, I, J, K, L, M), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M); A, B, C, D, E, F, G, H, I, J, K, L, M);
auto_function!((A, B, C, D, E, F, G, H, I, J, K, L, M, N), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>, Ref<N>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M, &'a N); A, B, C, D, E, F, G, H, I, J, K, L, M, N);
auto_function!((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>, Ref<N>, Ref<O>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M, &'a N, &'a O); A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
auto_function!((A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P), (Ref<A>, Ref<B>, Ref<C>, Ref<D>, Ref<E>, Ref<F>, Ref<G>, Ref<H>, Ref<I>, Ref<J>, Ref<K>, Ref<L>, Ref<M>, Ref<N>, Ref<O>, Ref<P>), (&'a A, &'a B, &'a C, &'a D, &'a E, &'a F, &'a G, &'a H, &'a I, &'a J, &'a K, &'a L, &'a M, &'a N, &'a O, &'a P); A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
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
        flow: &mut super::flow::Flow<'n>,
        input: Self::Input,
    ) -> <Self::Output as NodeOutput>::Refs
    where
        Self: 'n,
    {
        flow.then::<_, Self::Input, StructCall>(self, input)
    }
}

struct Factory<F, T>(F, PhantomData<fn() -> T>);
impl<F: Fn() -> T, T: Data> Node for Factory<F, T> {
    type Input = ();
    type Output = T;
    async fn run(&self, _: Query<&()>) -> Result<T, BodyError> {
        Ok((self.0)())
    }
}

pub struct Flow<'n> {
    builder: Rc<RefCell<Builder<'n>>>,
    body: BodyId,
    errors: Rc<RefCell<Vec<&'static str>>>,
    output: Option<Vec<PositionDesc>>,
}
impl<'n> Flow<'n> {
    fn open(
        builder: Rc<RefCell<Builder<'n>>>,
        body: BodyId,
        errors: Rc<RefCell<Vec<&'static str>>>,
    ) -> Self {
        Self {
            builder,
            body,
            errors,
            output: None,
        }
    }
    fn error(&self, note: &'static str) {
        self.errors.borrow_mut().push(note);
    }
    fn resolve<R: ShapeOps>(&self) -> R {
        let handles = R::type_descs()
            .into_iter()
            .map(|ty| {
                let b = self.builder.borrow();
                b.probe_resolve(self.body, ty).unwrap_or_else(|| {
                    self.error("missing visible source");
                    b.probe_missing()
                })
            })
            .collect::<Vec<_>>();
        R::from_positions(&mut handles.into_iter())
    }
    pub fn ref_of<T: Data>(&self) -> Ref<T> {
        self.resolve::<Ref<T>>()
    }
    pub fn then<N, K>(&mut self, node: N) -> &mut Self
    where
        N: AutoCallable<K> + 'n,
        N::Input: ShapeOps,
    {
        self.then_output(node);
        self
    }
    pub fn then_output<N, K>(&mut self, node: N) -> <N::Output as NodeOutput>::Refs
    where
        N: AutoCallable<K> + 'n,
        N::Input: ShapeOps,
    {
        let types = N::Input::type_descs();
        if types
            .iter()
            .enumerate()
            .any(|(i, t)| types[..i].contains(t))
        {
            self.error("duplicate input types in automatic Query; use then_with");
        }
        let input = self.resolve::<N::Input>();
        node.register(
            &mut super::flow::Flow::open(Rc::clone(&self.builder), self.body),
            input,
        )
    }
    pub fn then_with<N, A, K: 'static>(&mut self, node: N, refs: A) -> &mut Self
    where
        N: Callable<A, K> + 'n,
        A: ShapeOps,
        A::Spec: InputOps,
        N::Output: OutputOps,
        <N::Output as NodeOutput>::Refs: ShapeOps,
    {
        self.then_with_output(node, refs);
        self
    }
    pub fn then_with_output<N, A, K: 'static>(
        &mut self,
        node: N,
        refs: A,
    ) -> <N::Output as NodeOutput>::Refs
    where
        N: Callable<A, K> + 'n,
        A: ShapeOps,
        A::Spec: InputOps,
        N::Output: OutputOps,
        <N::Output as NodeOutput>::Refs: ShapeOps,
    {
        self.builder
            .borrow_mut()
            .register_node_step::<N, A, K>(self.body, node, refs)
    }
    pub fn provide<F: Fn() -> T + 'n, T: Data>(&mut self, factory: F) -> &mut Self {
        self.provide_output(factory);
        self
    }
    pub fn provide_output<F: Fn() -> T + 'n, T: Data>(&mut self, factory: F) -> Ref<T> {
        self.then_with_output(Factory(factory, PhantomData), ())
    }
    pub fn output<O: RootInput>(&mut self) -> &mut Self
    where
        O::Refs: ShapeOps,
    {
        let refs = self.resolve::<O::Refs>();
        self.output_with(refs)
    }
    pub fn output_with<R: ShapeOps>(&mut self, refs: R) -> &mut Self {
        if self.output.is_some() {
            self.error("duplicate output declaration");
        } else {
            self.output = Some(refs.positions());
        }
        self
    }
    pub fn chain<F: FnOnce(&mut Flow<'n>)>(&mut self, define: F) -> &mut Self {
        let child = self.builder.borrow_mut().new_body(Some(self.body));
        let mut sub = Self::open(Rc::clone(&self.builder), child, Rc::clone(&self.errors));
        define(&mut sub);
        self.builder.borrow_mut().register_chain_outputs(
            self.body,
            child,
            sub.output.take().unwrap_or_default(),
        );
        self
    }
}

pub(crate) fn build_root<'n, I, O, F>(define: F) -> Result<Definition<'n>, BuildError>
where
    I: RootInput + InputOps,
    I::Refs: ShapeOps,
    O: RootInput,
    O::Refs: ShapeOps,
    F: FnOnce(&mut Flow<'n>, I::Refs),
{
    let builder = Rc::new(RefCell::new(Builder::new()));
    let root = builder.borrow_mut().new_body(None);
    let inputs = builder.borrow_mut().probe_inputs::<I>(root);
    let refs = I::Refs::from_positions(&mut inputs.into_iter());
    let errors = Rc::new(RefCell::new(Vec::new()));
    let mut flow = Flow::open(Rc::clone(&builder), root, Rc::clone(&errors));
    define(&mut flow, refs);
    let output = flow.output.take();
    let expected = O::Refs::type_descs();
    if output.is_none() && !expected.is_empty() {
        flow.error("missing Root output declaration");
    }
    let output = output.unwrap_or_default();
    if output
        .iter()
        .map(|p| p.type_desc)
        .collect::<Vec<TypeDesc>>()
        != expected
    {
        flow.error("Root declared output does not match expected owned output");
    }
    drop(flow);
    if let Some(note) = errors.borrow().first().copied() {
        return Err(BuildError::new(BuildErrorDetail::ProbeContract { note }));
    }
    Rc::try_unwrap(builder)
        .ok()
        .expect("builder handle cannot escape Flow")
        .into_inner()
        .probe_finish(output)
        .map_err(BuildError::new)
}
