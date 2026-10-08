use crate::{
    core::{
        identity::{ExecutionIdentity, ScopeId},
        ref_id::{RefId, RefIdAllocator, RefIdSource},
        scope::{ExportSlot, ImportSlot, ScopeCoordinator},
        signature::DeclaredPort,
    },
    error::{BuildError, ControlSignal, RunError, Signal},
    shape::{
        Callable, Data, InputSpec, NodeOutput, Query, Raw, Ref, RefShape, RootInput, StateShape,
    },
};
use std::{
    any::TypeId,
    cell::RefCell,
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
};
type Task<'a> = Pin<Box<dyn Future<Output = Result<(), Signal>> + 'a>>;
struct Port {
    raw: Raw,
    id: RefId,
    ty: TypeId,
    name: &'static str,
}
struct ScopeDef {
    parent: Option<usize>,
    imports: Vec<(Raw, Raw)>,
    aliases: HashMap<Raw, Raw>,
}
struct Schema {
    identity: u64,
    allocator: RefIdAllocator,
    ports: Vec<Port>,
    scopes: Vec<ScopeDef>,
    errors: Vec<BuildError>,
}
static DEFINITIONS: AtomicU64 = AtomicU64::new(1);
impl Schema {
    fn new() -> Self {
        Self {
            identity: DEFINITIONS
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .expect("probe Definition identity space"),
            allocator: RefIdAllocator::new(RefIdSource::new()),
            ports: vec![],
            scopes: vec![ScopeDef {
                parent: None,
                imports: vec![],
                aliases: HashMap::new(),
            }],
            errors: vec![],
        }
    }
    fn allocate(&mut self, scope: usize, ty: TypeId, name: &'static str) -> Raw {
        let raw = Raw {
            definition: self.identity,
            scope,
            slot: self.ports.len(),
        };
        self.ports.push(Port {
            raw,
            id: self
                .allocator
                .allocate()
                .expect("probe Definition ID space"),
            ty,
            name,
        });
        raw
    }
    fn ensure(&mut self, scope: usize, raw: Raw) -> Raw {
        if raw.definition != self.identity {
            self.errors.push(BuildError::ForeignRef);
            return raw;
        }
        if self.ports.get(raw.slot).map(|p| p.raw) != Some(raw) {
            self.errors.push(BuildError::InvisibleRef);
            return raw;
        }
        if raw.scope == scope {
            return raw;
        }
        if let Some(alias) = self.scopes[scope].aliases.get(&raw) {
            return *alias;
        }
        let mut ancestor = self.scopes[scope].parent;
        let mut visible = false;
        while let Some(s) = ancestor {
            if s == raw.scope {
                visible = true;
                break;
            }
            ancestor = self.scopes[s].parent;
        }
        if !visible {
            self.errors.push(BuildError::InvisibleRef);
            return raw;
        }
        let parent = self.scopes[scope].parent.unwrap();
        let source = self.ensure(parent, raw);
        let p = &self.ports[raw.slot];
        let alias = self.allocate(scope, p.ty, p.name);
        self.scopes[scope].imports.push((source, alias));
        self.scopes[scope].aliases.insert(raw, alias);
        alias
    }
}

pub(crate) struct Run {
    pub core: ScopeCoordinator,
    schema: Rc<RefCell<Schema>>,
}
impl Run {
    pub fn id(&self, r: Raw) -> RefId {
        self.schema.borrow().ports[r.slot].id.clone()
    }
    fn import(&mut self, logical: usize, child: &ScopeId, parent: &ScopeId) -> Result<(), Signal> {
        let slots: Vec<_> = {
            let s = self.schema.borrow();
            s.scopes[logical]
                .imports
                .iter()
                .map(|(a, b)| {
                    let p = &s.ports[b.slot];
                    ImportSlot::with_type(&s.ports[a.slot].id, p.id.clone(), p.ty, p.name)
                })
                .collect()
        };
        self.core.import_batch(child, parent, &slots)?;
        Ok(())
    }
    fn begin(&mut self, logical: usize, parent: &ScopeId) -> Result<ScopeId, Signal> {
        let s = self.core.create_child(parent)?;
        self.import(logical, &s, parent)?;
        Ok(s)
    }
    fn finish(&mut self, scope: &ScopeId, from: &[Raw], to: &[Raw]) -> Result<(), Signal> {
        let declared: Vec<_> = from.iter().map(|r| self.id(*r)).collect();
        let mut exports: Vec<_> = from
            .iter()
            .zip(to)
            .map(|(a, b)| {
                let s = self.schema.borrow();
                let p = &s.ports[b.slot];
                ExportSlot::with_type(&s.ports[a.slot].id, p.id.clone(), p.ty, p.name)
            })
            .collect();
        self.core.finalize(scope, &declared, &mut exports)?;
        Ok(())
    }
    fn abort(&mut self, scope: &ScopeId) -> Result<(), Signal> {
        self.core.abort(scope)?;
        Ok(())
    }
}
trait Step {
    fn execute<'a>(&'a self, r: &'a mut Run, scope: ScopeId) -> Task<'a>;
}
struct Body<'n> {
    scope: usize,
    steps: Vec<Box<dyn Step + 'n>>,
    output: Vec<Raw>,
}
impl Body<'_> {
    fn execute<'a>(&'a self, r: &'a mut Run, scope: ScopeId) -> Task<'a> {
        Box::pin(async move {
            for step in &self.steps {
                step.execute(r, scope.clone()).await?;
            }
            Ok(())
        })
    }
}

/// Synchronous Definition context. Node registrations may borrow for 'n.
pub struct Flow<'n> {
    schema: Rc<RefCell<Schema>>,
    scope: usize,
    steps: Vec<Box<dyn Step + 'n>>,
}
impl<'n> Flow<'n> {
    fn root() -> Self {
        Self {
            schema: Rc::new(RefCell::new(Schema::new())),
            scope: 0,
            steps: vec![],
        }
    }
    fn child(&self) -> Self {
        let scope = {
            let mut s = self.schema.borrow_mut();
            let id = s.scopes.len();
            s.scopes.push(ScopeDef {
                parent: Some(self.scope),
                imports: vec![],
                aliases: HashMap::new(),
            });
            id
        };
        Self {
            schema: Rc::clone(&self.schema),
            scope,
            steps: vec![],
        }
    }
    pub(crate) fn allocate(&mut self, t: TypeId, n: &'static str) -> Raw {
        self.schema.borrow_mut().allocate(self.scope, t, n)
    }
    pub(crate) fn ensure(&mut self, r: Raw) -> Raw {
        self.schema.borrow_mut().ensure(self.scope, r)
    }
    fn body<O: RefShape>(mut self, o: O) -> Body<'n> {
        let o = o.local(&mut self);
        Body {
            scope: self.scope,
            steps: self.steps,
            output: o.raw(),
        }
    }
    pub fn then<N, A, K>(&mut self, node: N, args: A) -> <N::Output as NodeOutput>::Refs
    where
        A: RefShape,
        N: Callable<A, K> + 'n,
        K: 'n,
    {
        let args = args.local(self);
        let output = <<N as Callable<A, K>>::Output as NodeOutput>::Refs::fresh(self);
        self.steps.push(Box::new(NodeStep::<N, A, K> {
            node,
            args,
            output,
            marker: PhantomData,
        }));
        output
    }
    pub fn chain<F, O>(&mut self, f: F) -> O
    where
        O: RefShape,
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        let mut child = self.child();
        let out = f(&mut child);
        let body = child.body(out);
        let output = O::fresh(self);
        self.steps.push(Box::new(ChainStep {
            body,
            output: output.raw(),
        }));
        output
    }
    pub fn retry<F, O>(&mut self, max_retries: usize, f: F) -> O
    where
        O: RefShape,
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        if max_retries == usize::MAX {
            self.schema
                .borrow_mut()
                .errors
                .push(BuildError::RetryLimitOverflow)
        }
        let mut control = self.child();
        let mut attempt = control.child();
        let out = f(&mut attempt);
        let body = attempt.body(out);
        let local = O::fresh(&mut control);
        let output = O::fresh(self);
        self.steps.push(Box::new(RetryStep {
            scope: control.scope,
            body,
            local: local.raw(),
            output: output.raw(),
            max_retries,
        }));
        output
    }
    pub fn iter<S: StateShape, F>(&mut self, initial: S, max_iterations: usize, f: F) -> S
    where
        F: FnOnce(S, &mut Flow<'n>) -> S,
    {
        if max_iterations == 0 {
            self.schema
                .borrow_mut()
                .errors
                .push(BuildError::InvalidIterationLimit)
        }
        let mut control = self.child();
        let initial = initial.local(&mut control);
        let mut round = control.child();
        let current = S::fresh(&mut round);
        let next = f(current, &mut round);
        let body = round.body(next);
        let local = S::fresh(&mut control);
        let output = S::fresh(self);
        self.steps.push(Box::new(IterStep::<S> {
            scope: control.scope,
            initial,
            current,
            body,
            local,
            output,
            max_iterations,
        }));
        output
    }
    pub fn each<T: Data, F, O>(&mut self, items: Ref<Vec<T>>, f: F) -> O::Collected
    where
        O: RefShape,
        F: FnOnce(Ref<T>, &mut Flow<'n>) -> O,
    {
        let mut control = self.child();
        let items = items.local(&mut control);
        let mut item_flow = control.child();
        let item = Ref::<T>::fresh(&mut item_flow);
        let out = f(item, &mut item_flow);
        let body = item_flow.body(out);
        let local = O::Collected::fresh(&mut control);
        let output = O::Collected::fresh(self);
        self.steps.push(Box::new(EachStep::<T, O> {
            scope: control.scope,
            items,
            item,
            body,
            local,
            output,
            marker: PhantomData,
        }));
        output
    }
    pub fn choose<K: Data + PartialEq, F, O>(&mut self, selector: Ref<K>, f: F) -> O
    where
        O: RefShape,
        F: FnOnce(&mut Choice<'n, K, O>),
    {
        let mut control = self.child();
        let selector = selector.local(&mut control);
        let mut choice = Choice {
            flow: control,
            cases: vec![],
            otherwise: None,
            marker: PhantomData,
        };
        f(&mut choice);
        if choice.cases.is_empty() && choice.otherwise.is_none() {
            self.schema
                .borrow_mut()
                .errors
                .push(BuildError::EmptyChoose)
        }
        let local = O::fresh(&mut choice.flow);
        let output = O::fresh(self);
        self.steps.push(Box::new(ChooseStep {
            scope: choice.flow.scope,
            selector,
            cases: choice.cases,
            otherwise: choice.otherwise,
            local: local.raw(),
            output: output.raw(),
        }));
        output
    }
}
pub struct Choice<'n, K, O> {
    flow: Flow<'n>,
    cases: Vec<(K, Body<'n>)>,
    otherwise: Option<Body<'n>>,
    marker: PhantomData<O>,
}
impl<'n, K: PartialEq, O: RefShape> Choice<'n, K, O> {
    pub fn case<F>(&mut self, key: K, f: F)
    where
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        if self.cases.iter().any(|(k, _)| k == &key) {
            self.flow
                .schema
                .borrow_mut()
                .errors
                .push(BuildError::DuplicateCase)
        }
        let mut branch = self.flow.child();
        let out = f(&mut branch);
        self.cases.push((key, branch.body(out)));
    }
    pub fn otherwise<F>(&mut self, f: F)
    where
        F: FnOnce(&mut Flow<'n>) -> O,
    {
        if self.otherwise.is_some() {
            self.flow
                .schema
                .borrow_mut()
                .errors
                .push(BuildError::DuplicateOtherwise)
        }
        let mut branch = self.flow.child();
        let out = f(&mut branch);
        self.otherwise = Some(branch.body(out));
    }
}

struct NodeStep<N: Callable<A, K>, A: RefShape, K> {
    node: N,
    args: A,
    output: <N::Output as NodeOutput>::Refs,
    marker: PhantomData<K>,
}
impl<N, A, K> Step for NodeStep<N, A, K>
where
    A: RefShape,
    N: Callable<A, K>,
{
    fn execute<'a>(&'a self, r: &'a mut Run, s: ScopeId) -> Task<'a> {
        Box::pin(async move {
            let raw = self.args.raw();
            let output = {
                let borrowed = A::Spec::read(r, &s, &raw)?;
                self.node
                    .call(Query::new(borrowed))
                    .await
                    .map_err(Signal::from)?
            };
            output.store(r, &s, self.output)
        })
    }
}
struct ChainStep<'n> {
    body: Body<'n>,
    output: Vec<Raw>,
}
impl Step for ChainStep<'_> {
    fn execute<'a>(&'a self, r: &'a mut Run, parent: ScopeId) -> Task<'a> {
        Box::pin(async move {
            let child = r.begin(self.body.scope, &parent)?;
            match self.body.execute(r, child.clone()).await {
                Ok(()) => r.finish(&child, &self.body.output, &self.output),
                Err(e) => {
                    r.abort(&child)?;
                    Err(e)
                }
            }
        })
    }
}
struct RetryStep<'n> {
    scope: usize,
    body: Body<'n>,
    local: Vec<Raw>,
    output: Vec<Raw>,
    max_retries: usize,
}
impl Step for RetryStep<'_> {
    fn execute<'a>(&'a self, r: &'a mut Run, parent: ScopeId) -> Task<'a> {
        Box::pin(async move {
            let control = r.begin(self.scope, &parent)?;
            for i in 0..=self.max_retries {
                let attempt = r.begin(self.body.scope, &control)?;
                match self.body.execute(r, attempt.clone()).await {
                    Ok(()) => {
                        r.finish(&attempt, &self.body.output, &self.local)?;
                        return r.finish(&control, &self.local, &self.output);
                    }
                    Err(Signal::Control(ControlSignal::Retry(error))) => {
                        r.abort(&attempt)?;
                        if i == self.max_retries {
                            r.abort(&control)?;
                            return Err(Signal::Terminal(RunError::RetryExhausted {
                                max_retries: self.max_retries,
                                attempts: i + 1,
                                last_error: error,
                            }));
                        }
                    }
                    Err(e) => {
                        r.abort(&control)?;
                        return Err(e);
                    }
                }
            }
            unreachable!("validated bounded retry loop")
        })
    }
}
struct IterStep<'n, S> {
    scope: usize,
    initial: S,
    current: S,
    body: Body<'n>,
    local: S,
    output: S,
    max_iterations: usize,
}
impl<S: StateShape> Step for IterStep<'_, S> {
    fn execute<'a>(&'a self, r: &'a mut Run, parent: ScopeId) -> Task<'a> {
        Box::pin(async move {
            let control = r.begin(self.scope, &parent)?;
            let states = self.initial.register(r, &control)?;
            for _ in 0..self.max_iterations {
                let round = r.begin(self.body.scope, &control)?;
                r.core.import_batch_with_states(
                    &round,
                    &control,
                    &[],
                    &self.current.import(r, &states),
                )?;
                match self.body.execute(r, round.clone()).await {
                    Ok(()) => {
                        let positions: Vec<_> = self.body.output.iter().map(|k| r.id(*k)).collect();
                        if states.len() == 1 {
                            r.core.promote(&round, &positions[0], &states[0])?
                        } else {
                            r.core.promote_group_probe(&round, &positions, &states)?
                        }
                        r.core.recycle_pending(&control)?;
                    }
                    Err(Signal::Control(ControlSignal::IterBreak(_))) => {
                        r.abort(&round)?;
                        let local = self.local.raw();
                        for (key, state) in local.iter().zip(&states) {
                            r.core.bind_state_output(&control, state, &r.id(*key))?;
                        }
                        return r.finish(&control, &local, &self.output.raw());
                    }
                    Err(e) => {
                        r.abort(&control)?;
                        return Err(e);
                    }
                }
            }
            r.abort(&control)?;
            Err(Signal::Terminal(RunError::IterationLimitReached {
                max_iterations: self.max_iterations,
                completed_iterations: self.max_iterations,
            }))
        })
    }
}
struct EachStep<'n, T, O: RefShape> {
    scope: usize,
    items: Ref<Vec<T>>,
    item: Ref<T>,
    body: Body<'n>,
    local: O::Collected,
    output: O::Collected,
    marker: PhantomData<O>,
}
impl<T: Data, O: RefShape> Step for EachStep<'_, T, O> {
    fn execute<'a>(&'a self, r: &'a mut Run, parent: ScopeId) -> Task<'a> {
        Box::pin(async move {
            let control = r.begin(self.scope, &parent)?;
            let collectors = O::collectors(r, &control)?;
            let len = r
                .core
                .resolve::<Vec<T>>(&control, &r.id(self.items.raw))?
                .len();
            for i in 0..len {
                let item = r.begin(self.body.scope, &control)?;
                r.core.bind_item_input::<T>(
                    &item,
                    &control,
                    &r.id(self.items.raw),
                    &r.id(self.item.raw),
                    i,
                )?;
                if let Err(e) = self.body.execute(r, item.clone()).await {
                    r.abort(&control)?;
                    return Err(e);
                }
                let positions: Vec<_> = self.body.output.iter().map(|k| r.id(*k)).collect();
                if collectors.is_empty() {
                    r.finish(&item, &[], &[])?
                } else if collectors.len() == 1 {
                    r.core.consume_item(&item, &positions[0], &collectors[0])?
                } else {
                    r.core
                        .consume_item_group_probe(&item, &positions, &collectors)?
                }
            }
            let local = self.local.raw();
            for (key, collector) in local.iter().zip(&collectors) {
                r.core.finish_collector(&control, collector, &r.id(*key))?;
            }
            r.finish(&control, &local, &self.output.raw())
        })
    }
}
struct ChooseStep<'n, K> {
    scope: usize,
    selector: Ref<K>,
    cases: Vec<(K, Body<'n>)>,
    otherwise: Option<Body<'n>>,
    local: Vec<Raw>,
    output: Vec<Raw>,
}
impl<K: Data + PartialEq> Step for ChooseStep<'_, K> {
    fn execute<'a>(&'a self, r: &'a mut Run, parent: ScopeId) -> Task<'a> {
        Box::pin(async move {
            let control = r.begin(self.scope, &parent)?;
            let selected = {
                let key = r.core.resolve::<K>(&control, &r.id(self.selector.raw))?;
                self.cases.iter().position(|(k, _)| k == key)
            };
            let body = selected
                .map(|i| &self.cases[i].1)
                .or(self.otherwise.as_ref());
            let Some(body) = body else {
                r.abort(&control)?;
                return Err(Signal::Terminal(RunError::NoMatchingCase));
            };
            let branch = r.begin(body.scope, &control)?;
            match body.execute(r, branch.clone()).await {
                Ok(()) => {
                    r.finish(&branch, &body.output, &self.local)?;
                    r.finish(&control, &self.local, &self.output)
                }
                Err(e) => {
                    r.abort(&control)?;
                    Err(e)
                }
            }
        })
    }
}

#[derive(Default)]
pub struct Runtime;
impl Runtime {
    pub fn new() -> Self {
        Self
    }
    pub async fn execute<'n, I, O, F>(&self, build: F, input: I) -> Result<O::Owned, RunError>
    where
        I: RootInput,
        O: RefShape,
        F: FnOnce(&mut Flow<'n>, I::Refs) -> O,
    {
        let mut flow = Flow::root();
        let inputs = I::Refs::fresh(&mut flow);
        let output = build(&mut flow, inputs).local(&mut flow);
        let declared = output.raw();
        if declared
            .iter()
            .enumerate()
            .any(|(i, k)| declared[..i].contains(k))
        {
            flow.schema
                .borrow_mut()
                .errors
                .push(BuildError::DuplicateRootRef);
        }
        if !flow.schema.borrow().errors.is_empty() {
            let e = flow.schema.borrow_mut().errors.remove(0);
            return Err(RunError::Definition(e));
        }
        let schema = Rc::clone(&flow.schema);
        let body = flow.body(output);
        let mut run = Run {
            core: ScopeCoordinator::new(ExecutionIdentity::new()),
            schema,
        };
        let root = run.core.root();
        for (key, (name, value)) in inputs.raw().into_iter().zip(input.values()) {
            run.core
                .register_owned_erased(&root, &run.id(key), name, value)
                .map_err(|e| RunError::Runtime(Box::new(e)))?;
        }
        if let Err(e) = body.execute(&mut run, root.clone()).await {
            run.abort(&root).map_err(RunError::from)?;
            return Err(e.into());
        }
        let ports: Vec<_> = {
            let s = run.schema.borrow();
            body.output
                .iter()
                .map(|k| {
                    let p = &s.ports[k.slot];
                    DeclaredPort::with_type(p.id.clone(), p.ty, p.name)
                })
                .collect()
        };
        run.core
            .freeze_root(&root)
            .map_err(|e| RunError::Runtime(Box::new(e)))?;
        let plan = run
            .core
            .prepare_root_extraction(&root, &ports)
            .map_err(|e| RunError::Runtime(Box::new(e)))?;
        let (values, remaining) = run.core.commit_root_extraction(&root, plan);
        run.core
            .close_root_validated(&root, remaining)
            .map_err(|e| RunError::Runtime(Box::new(e)))?;
        Ok(O::decode(&mut values.into_iter()))
    }
}
