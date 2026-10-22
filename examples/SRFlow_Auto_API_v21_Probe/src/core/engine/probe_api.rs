//! Experimental API over the existing Definition and execution engine.
//! No runtime type registry or alternative execution plan is introduced.
#![allow(missing_docs, private_bounds)]

use super::definition::{
    BeginCollectorsFn, Builder, ChooseCase, ChooseStep, Definition, FinishCollectorsFn,
};
use super::identity::{BodyId, PositionHandle};
use super::node::{Callable, FunctionCall, StructCall};
use super::shape::{
    self, EachCollect, InputOps, OutputOps, PositionDesc, RootInputOps, ShapeOps, StateShape,
    TypeDesc,
};
use crate::{BodyError, Data, Node, NodeOutput, Query, Ref, RefShape, RootInput, RunError};
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

/// Probe diagnostic carrier; production error variants are not frozen by this experiment.
#[derive(Debug)]
pub enum ProbeError {
    Definition(Vec<String>),
    Execution(RunError),
}
impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ProbeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Execution(e) => Some(e),
            _ => None,
        }
    }
}

/// Reuses the existing trait bridge. The marker exposes concrete borrowed signatures
/// to inference; the original Callable projection alone does not infer the input shape.
#[doc(hidden)]
pub trait AutoCallable<K> {
    type Input: RefShape;
    type Output: NodeOutput;
    fn register<'n>(
        self,
        flow: &mut crate::Flow<'n>,
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
    fn register<'n>(self, flow: &mut crate::Flow<'n>, input: ()) -> O::Refs
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
            fn register<'n>(self, flow: &mut crate::Flow<'n>, input: Self::Input) -> Out::Refs where Self:'n {
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
        flow: &mut crate::Flow<'n>,
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

#[derive(Clone)]
struct Declaration {
    slots: Vec<PositionDesc>,
    collected: Vec<TypeDesc>,
    begin: BeginCollectorsFn,
    finish: FinishCollectorsFn,
}
impl Declaration {
    fn of<R>(refs: R) -> Self
    where
        R: ShapeOps + EachCollect,
        R::Collected: ShapeOps,
    {
        Self {
            slots: refs.positions(),
            collected: <R::Collected as ShapeOps>::type_descs(),
            begin: R::begin_collectors,
            finish: R::finish_collectors,
        }
    }
    fn unit() -> Self {
        Self::of(())
    }
    fn types(&self) -> Vec<TypeDesc> {
        self.slots.iter().map(|p| p.type_desc).collect()
    }
}

/// Synchronous builder with a single explicit output/next declaration per Body.
#[derive(Clone, Debug)]
pub struct BodyContract {
    pub output_declared: bool,
    pub next_declared: bool,
}
#[derive(Default)]
struct BuildState {
    errors: Vec<String>,
    contracts: Vec<BodyContract>,
}

pub struct Flow<'n> {
    builder: Rc<RefCell<Builder<'n>>>,
    body: BodyId,
    errors: Rc<RefCell<BuildState>>,
    output: Option<Declaration>,
    next: Option<Vec<PositionDesc>>,
    iter_round: bool,
}
impl<'n> Flow<'n> {
    fn open(
        builder: Rc<RefCell<Builder<'n>>>,
        body: BodyId,
        errors: Rc<RefCell<BuildState>>,
        iter_round: bool,
    ) -> Self {
        {
            let mut state = errors.borrow_mut();
            while state.contracts.len() <= body.index() {
                state.contracts.push(BodyContract {
                    output_declared: false,
                    next_declared: false,
                });
            }
        }
        Self {
            builder,
            body,
            errors,
            output: None,
            next: None,
            iter_round,
        }
    }
    fn error(&self, message: impl Into<String>) {
        self.errors.borrow_mut().errors.push(message.into());
    }
    fn resolve<R: ShapeOps>(&self) -> R {
        let handles = R::type_descs()
            .into_iter()
            .map(|ty| {
                let b = self.builder.borrow();
                b.probe_resolve(self.body, ty).unwrap_or_else(|| {
                    self.error(format!("missing visible source: {}", ty));
                    b.probe_missing()
                })
            })
            .collect::<Vec<_>>();
        R::from_positions(&mut handles.into_iter())
    }
    /// Returns the latest legal definition position, without registering a step or import.
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
        let input = self.resolve::<N::Input>();
        node.register(
            &mut crate::Flow::open(Rc::clone(&self.builder), self.body),
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
    pub fn then_with_output<N, A, K>(&mut self, node: N, refs: A) -> <N::Output as NodeOutput>::Refs
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
        O::Refs: ShapeOps + EachCollect,
        <O::Refs as shape::EachShape>::Collected: ShapeOps,
    {
        let refs = self.resolve::<O::Refs>();
        self.output_with(refs)
    }
    pub fn output_with<R>(&mut self, refs: R) -> &mut Self
    where
        R: ShapeOps + EachCollect,
        R::Collected: ShapeOps,
    {
        if self.iter_round {
            self.error("Iter round uses next, not output");
        }
        if self.output.is_some() {
            self.error("duplicate output declaration");
        } else {
            self.output = Some(Declaration::of(refs));
            self.errors.borrow_mut().contracts[self.body.index()].output_declared = true;
        }
        self
    }
    #[allow(clippy::should_implement_trait)] // User-selected Iter contract name, not an iterator.
    pub fn next<S: RootInput>(&mut self) -> &mut Self
    where
        S::Refs: StateShape,
    {
        let refs = self.resolve::<S::Refs>();
        self.next_with(refs)
    }
    pub fn next_with<R: StateShape>(&mut self, refs: R) -> &mut Self {
        if !self.iter_round {
            self.error("next outside Iter round");
        }
        if self.next.is_some() {
            self.error("duplicate next declaration");
        } else {
            self.next = Some(refs.positions());
            self.errors.borrow_mut().contracts[self.body.index()].next_declared = true;
        }
        self
    }
    fn child<F: FnOnce(&mut Flow<'n>)>(&self, define: F) -> (BodyId, Declaration) {
        let child = self.builder.borrow_mut().new_body(Some(self.body));
        let mut sub = Self::open(
            Rc::clone(&self.builder),
            child,
            Rc::clone(&self.errors),
            false,
        );
        define(&mut sub);
        (child, sub.output.take().unwrap_or_else(Declaration::unit))
    }
    fn shape<R: ShapeOps>(&self, slots: Vec<PositionHandle>, types: Vec<TypeDesc>) -> R {
        if R::type_descs() != types {
            self.error("requested RefShape does not match declared output");
            return R::from_positions(&mut std::iter::repeat(
                self.builder.borrow().probe_missing(),
            ));
        }
        R::from_positions(&mut slots.into_iter())
    }
    fn chain_slots<F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        define: F,
    ) -> (Vec<PositionHandle>, Vec<TypeDesc>) {
        let (child, declaration) = self.child(define);
        let types = declaration.types();
        let handles =
            self.builder
                .borrow_mut()
                .register_chain_outputs(self.body, child, declaration.slots);
        (handles, types)
    }
    pub fn chain<F: FnOnce(&mut Flow<'n>)>(&mut self, define: F) -> &mut Self {
        self.chain_slots(define);
        self
    }
    pub fn chain_output<R: ShapeOps, F: FnOnce(&mut Flow<'n>)>(&mut self, define: F) -> R {
        let (handles, types) = self.chain_slots(define);
        self.shape(handles, types)
    }
    fn retry_slots<F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        budget: usize,
        define: F,
    ) -> (Vec<PositionHandle>, Vec<TypeDesc>) {
        let (child, declaration) = self.child(define);
        let types = declaration.types();
        let handles =
            self.builder
                .borrow_mut()
                .register_retry(self.body, child, declaration.slots, budget);
        (handles, types)
    }
    pub fn retry<F: FnOnce(&mut Flow<'n>)>(&mut self, budget: usize, define: F) -> &mut Self {
        self.retry_slots(budget, define);
        self
    }
    pub fn retry_output<R: ShapeOps, F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        budget: usize,
        define: F,
    ) -> R {
        let (handles, types) = self.retry_slots(budget, define);
        self.shape(handles, types)
    }
    fn each_slots<T: Data, F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        items: Ref<Vec<T>>,
        define: F,
    ) -> (Vec<PositionHandle>, Vec<TypeDesc>) {
        let (step, child, item) = {
            let mut b = self.builder.borrow_mut();
            let step = b.step_count(self.body);
            let child = b.new_body(Some(self.body));
            let item = b.allocate_each_item(child, step, TypeDesc::of::<T>());
            (step, child, item)
        };
        let mut sub = Self::open(
            Rc::clone(&self.builder),
            child,
            Rc::clone(&self.errors),
            false,
        );
        define(&mut sub);
        let decl = sub.output.take().unwrap_or_else(Declaration::unit);
        let types = decl.collected.clone();
        let handles = self.builder.borrow_mut().register_each(
            self.body,
            child,
            step,
            PositionDesc {
                handle: items.handle(),
                type_desc: TypeDesc::of::<Vec<T>>(),
            },
            item,
            decl.slots,
            decl.collected,
            decl.begin,
            shape::each_bind_item::<T>,
            shape::each_collection_len::<T>,
            decl.finish,
        );
        (handles, types)
    }
    pub fn each<T: Data, F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        items: Ref<Vec<T>>,
        define: F,
    ) -> &mut Self {
        self.each_slots(items, define);
        self
    }
    pub fn each_output<T: Data, R: ShapeOps, F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        items: Ref<Vec<T>>,
        define: F,
    ) -> R {
        let (handles, types) = self.each_slots(items, define);
        self.shape(handles, types)
    }
    pub fn iter<S: StateShape, F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        initial: S,
        budget: usize,
        define: F,
    ) -> &mut Self {
        self.iter_output(initial, budget, define);
        self
    }
    pub fn iter_output<S: StateShape, F: FnOnce(&mut Flow<'n>)>(
        &mut self,
        initial: S,
        budget: usize,
        define: F,
    ) -> S {
        let (step, child, current) = {
            let mut b = self.builder.borrow_mut();
            let step = b.step_count(self.body);
            let child = b.new_body(Some(self.body));
            let current = b.allocate_iter_current(child, step, &S::type_descs());
            (step, child, current)
        };
        let mut sub = Self::open(
            Rc::clone(&self.builder),
            child,
            Rc::clone(&self.errors),
            true,
        );
        define(&mut sub);
        let next = sub.next.take().unwrap_or_else(|| {
            self.error("missing next declaration");
            Vec::new()
        });
        if next.iter().map(|p| p.type_desc).collect::<Vec<_>>() != S::type_descs() {
            self.error("next shape differs from initial state");
            return S::from_positions(&mut std::iter::repeat(
                self.builder.borrow().probe_missing(),
            ));
        }
        let handles = self.builder.borrow_mut().register_iter(
            self.body,
            child,
            step,
            initial.positions(),
            next,
            current,
            budget,
            S::register_states,
            S::state_import_slots,
        );
        S::from_positions(&mut handles.into_iter())
    }
    fn choose_slots<K: Data + PartialEq, F: FnOnce(&mut Choice<'n, K>)>(
        &mut self,
        selector: Option<Ref<K>>,
        define: F,
    ) -> (Vec<PositionHandle>, Vec<TypeDesc>) {
        let selector = selector.unwrap_or_else(|| self.ref_of::<K>());
        let (step, order) = {
            let mut b = self.builder.borrow_mut();
            (b.step_count(self.body), b.fresh_order())
        };
        let mut choice = Choice {
            builder: Rc::clone(&self.builder),
            parent: self.body,
            errors: Rc::clone(&self.errors),
            branches: Vec::new(),
        };
        define(&mut choice);
        let types = choice
            .branches
            .first()
            .map(|(_, _, d)| d.types())
            .unwrap_or_default();
        if choice.branches.is_empty() {
            self.error("empty Choose");
        }
        if choice.branches.iter().any(|(_, _, d)| d.types() != types) {
            self.error("Choose branch output shape mismatch");
        }
        let mut b = self.builder.borrow_mut();
        let outputs = b.allocate_choose_outputs(self.body, step, &types);
        let mut cases = Vec::new();
        let mut otherwise = None;
        for (index, (key, child, decl)) in choice.branches.into_iter().enumerate() {
            let branch = b.register_choose_branch(child, step, index as u16, decl.slots, &outputs);
            match key {
                Some(key) => cases.push(ChooseCase { key, branch }),
                None => otherwise = Some(branch),
            }
        }
        let selector = PositionDesc {
            handle: selector.handle(),
            type_desc: TypeDesc::of::<K>(),
        };
        b.record_position_use(self.body, selector);
        b.push_choose_step(
            self.body,
            ChooseStep::new(
                selector,
                cases,
                otherwise,
                outputs.iter().map(|h| h.position).collect(),
                order,
            ),
        );
        (outputs, types)
    }
    pub fn choose<K: Data + PartialEq, F: FnOnce(&mut Choice<'n, K>)>(
        &mut self,
        define: F,
    ) -> &mut Self {
        self.choose_slots(None, define);
        self
    }
    pub fn choose_with<K: Data + PartialEq, F: FnOnce(&mut Choice<'n, K>)>(
        &mut self,
        selector: Ref<K>,
        define: F,
    ) -> &mut Self {
        self.choose_slots(Some(selector), define);
        self
    }
    pub fn choose_output<K: Data + PartialEq, R: ShapeOps, F: FnOnce(&mut Choice<'n, K>)>(
        &mut self,
        define: F,
    ) -> R {
        let (handles, types) = self.choose_slots(None, define);
        self.shape(handles, types)
    }
    pub fn choose_with_output<K: Data + PartialEq, R: ShapeOps, F: FnOnce(&mut Choice<'n, K>)>(
        &mut self,
        selector: Ref<K>,
        define: F,
    ) -> R {
        let (handles, types) = self.choose_slots(Some(selector), define);
        self.shape(handles, types)
    }
    pub fn build<I, O>(define: impl FnOnce(&mut Flow<'n>, I::Refs)) -> Workflow<'n, I, O>
    where
        I: RootInput + InputOps,
        I::Refs: ShapeOps,
        O: RootInput,
        O::Refs: ShapeOps,
    {
        let builder = Rc::new(RefCell::new(Builder::new()));
        let root = builder.borrow_mut().new_body(None);
        let inputs = builder.borrow_mut().probe_inputs::<I>(root);
        let refs = I::Refs::from_positions(&mut inputs.into_iter());
        let errors = Rc::new(RefCell::new(BuildState::default()));
        let mut flow = Self::open(Rc::clone(&builder), root, Rc::clone(&errors), false);
        define(&mut flow, refs);
        let decl = flow.output.take();
        if decl.is_none() && !O::Refs::type_descs().is_empty() {
            flow.error("missing Root output declaration");
        }
        let decl = decl.unwrap_or_else(Declaration::unit);
        if decl.types() != O::Refs::type_descs() {
            flow.error("Root output shape does not match Workflow<I,O>");
        }
        drop(flow);
        let contracts = errors.borrow().contracts.clone();
        let errors = errors.borrow().errors.clone();
        let definition = if errors.is_empty() {
            Rc::try_unwrap(builder)
                .ok()
                .expect("builder handle cannot escape Flow")
                .into_inner()
                .probe_finish(decl.slots)
                .map_err(|e| ProbeError::Definition(vec![format!("{e:?}")]))
        } else {
            Err(ProbeError::Definition(errors))
        };
        Workflow {
            definition,
            contracts,
            marker: PhantomData,
        }
    }
}

pub struct Choice<'n, K> {
    builder: Rc<RefCell<Builder<'n>>>,
    parent: BodyId,
    errors: Rc<RefCell<BuildState>>,
    branches: Vec<(Option<K>, BodyId, Declaration)>,
}
impl<'n, K: Data + PartialEq> Choice<'n, K> {
    fn branch(&mut self, key: Option<K>, define: impl FnOnce(&mut Flow<'n>)) {
        if self.branches.iter().any(|(old, _, _)| old == &key) {
            self.errors
                .borrow_mut()
                .errors
                .push("duplicate Choose case/otherwise".into());
        }
        let child = self.builder.borrow_mut().new_body(Some(self.parent));
        let mut flow = Flow::open(
            Rc::clone(&self.builder),
            child,
            Rc::clone(&self.errors),
            false,
        );
        define(&mut flow);
        self.branches.push((
            key,
            child,
            flow.output.take().unwrap_or_else(Declaration::unit),
        ));
    }
    pub fn case(&mut self, key: K, define: impl FnOnce(&mut Flow<'n>)) {
        self.branch(Some(key), define);
    }
    pub fn otherwise(&mut self, define: impl FnOnce(&mut Flow<'n>)) {
        self.branch(None, define);
    }
}

/// Holds the existing immutable Definition; no input values or invocation state.
pub struct Workflow<'n, I: RootInput, O: RootInput> {
    definition: Result<Definition<'n>, ProbeError>,
    contracts: Vec<BodyContract>,
    marker: PhantomData<fn(I) -> O>,
}
impl<I: RootInput, O: RootInput> Workflow<'_, I, O> {
    pub fn contracts(&self) -> &[BodyContract] {
        &self.contracts
    }
    pub fn validate(&self) -> Result<(), &ProbeError> {
        self.definition.as_ref().map(|_| ())
    }
}
#[derive(Default)]
pub struct Runtime;
impl Runtime {
    pub fn new() -> Self {
        Self
    }
    pub async fn execute<I, O>(
        &self,
        workflow: &Workflow<'_, I, O>,
        input: I,
    ) -> Result<O, ProbeError>
    where
        I: RootInput + RootInputOps + InputOps,
        I::Refs: ShapeOps,
        O: RootInput<Owned = O>,
        O::Refs: ShapeOps + RefShape<Owned = O>,
    {
        let built = workflow
            .definition
            .as_ref()
            .map_err(|e| ProbeError::Definition(vec![e.to_string()]))?;
        crate::Runtime::new()
            .execute_built::<I, O::Refs>(built, input)
            .await
            .map_err(ProbeError::Execution)
    }
}

#[cfg(test)]
mod structural_tests {
    use super::*;
    async fn number(q: Query<&u32>) -> Result<u32, BodyError> {
        Ok(*q.get() + 1)
    }
    async fn pair(q: Query<(&u32, &u32)>) -> Result<(), BodyError> {
        let _ = q.get();
        Ok(())
    }
    #[test]
    fn ref_lookup_registers_neither_step_nor_import() {
        let workflow: Workflow<u32, ()> = Flow::build(|flow, _| {
            flow.chain(|sub| {
                let _ = sub.ref_of::<u32>();
            });
        });
        let built = workflow.definition.as_ref().unwrap();
        assert_eq!(built.root().steps().len(), 1);
        assert!(built.body(BodyId::new(1)).steps().is_empty());
        assert!(built.body(BodyId::new(1)).imports().is_empty());
        assert!(built.root().output().is_empty());
        assert_eq!(built.positions().len(), 1);
    }
    #[test]
    fn automatic_connections_store_concrete_immutable_positions() {
        let workflow: Workflow<u32, ()> = Flow::build(|flow, input| {
            flow.then(number).provide(|| 99u32).then(pair);
            // The original input remains a first-class explicit dependency.
            flow.then_with(number, input);
        });
        let built = workflow.definition.as_ref().unwrap();
        let steps = built.root().steps();
        assert_eq!(
            steps[0].inputs()[0].handle,
            built.root_inputs().next().unwrap().handle()
        );
        let provided = steps[1].outputs()[0];
        assert_eq!(steps[2].inputs()[0].handle.position, provided);
        assert_eq!(steps[2].inputs()[1].handle.position, provided);
        assert_eq!(
            steps[3].inputs()[0].handle,
            built.root_inputs().next().unwrap().handle()
        );
    }
    #[test]
    fn output_is_frozen_at_declaration_not_end_of_body() {
        let workflow: Workflow<u32, u32> = Flow::build(|flow, input| {
            flow.output_with(input);
            flow.provide(|| 100u32);
        });
        let built = workflow.definition.as_ref().unwrap();
        assert_eq!(
            built.root().output()[0].handle,
            built.root_inputs().next().unwrap().handle()
        );
    }
}
