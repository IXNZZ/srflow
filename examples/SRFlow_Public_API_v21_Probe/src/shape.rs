use crate::{
    BodyError,
    core::{
        identity::ScopeId,
        scope::{ControlStateId, StateImportSlot},
    },
    error::Signal,
    flow::{Flow, Run},
};
use std::{
    any::{Any, TypeId, type_name},
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
};

/// Explicit non-unit business Data declaration, separating () without specialization.
pub trait Data: Any {}
macro_rules! builtin { ($($t:ty),*) => { $(impl Data for $t {})* }; }
builtin!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, bool, char, String, f32, f64
);
impl<T: 'static + ?Sized> Data for Box<T> {}
impl<T: 'static> Data for Vec<T> {}
impl<T: 'static> Data for Option<T> {}
impl<T: 'static> Data for std::rc::Rc<T> {}
impl<T: 'static> Data for Arc<T> {}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Raw {
    pub definition: u64,
    pub scope: usize,
    pub slot: usize,
}
pub struct Ref<T> {
    pub(crate) raw: Raw,
    marker: PhantomData<fn() -> T>,
}
impl<T> Copy for Ref<T> {}
impl<T> Clone for Ref<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> std::fmt::Debug for Ref<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ref<{}>", type_name::<T>())
    }
}
impl<T> Ref<T> {
    pub(crate) fn from_raw(raw: Raw) -> Self {
        Self {
            raw,
            marker: PhantomData,
        }
    }
}
#[doc(hidden)]
pub trait QueryType {
    type View;
}
impl QueryType for () {
    type View = ();
}
impl<'a, T: InputSpec> QueryType for &'a T {
    type View = T::Borrowed<'a>;
}
macro_rules! query_types {($($t:ident:$i:tt),+)=>{
    impl<$($t),+> QueryType for ($($t,)+) {type View=Self;}
}}
query_types!(A:0,B:1);
query_types!(A:0,B:1,C:2);
query_types!(A:0,B:1,C:2,D:3);
query_types!(A:0,B:1,C:2,D:3,E:4);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14);
query_types!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14,P:15);
pub struct Query<T: QueryType> {
    view: T::View,
    marker: PhantomData<fn() -> T>,
}
impl<T: QueryType> Query<T> {
    pub(crate) fn new(view: T::View) -> Self {
        Self {
            view,
            marker: PhantomData,
        }
    }
    pub fn get(self) -> T::View {
        self.view
    }
}
pub(crate) fn descriptor<T: 'static>() -> (TypeId, &'static str) {
    (TypeId::of::<T>(), type_name::<T>())
}

pub trait InputSpec: 'static {
    type Borrowed<'a>: QueryType<View = Self::Borrowed<'a>>;
    #[doc(hidden)]
    fn types() -> Vec<(TypeId, &'static str)>;
    #[doc(hidden)]
    fn read<'a>(run: &'a Run, scope: &ScopeId, raw: &[Raw]) -> Result<Self::Borrowed<'a>, Signal>;
}
impl InputSpec for () {
    type Borrowed<'a> = ();
    fn types() -> Vec<(TypeId, &'static str)> {
        vec![]
    }
    fn read(_: &Run, _: &ScopeId, _: &[Raw]) -> Result<(), Signal> {
        Ok(())
    }
}
impl<T: Data> InputSpec for T {
    type Borrowed<'a> = &'a T;
    fn types() -> Vec<(TypeId, &'static str)> {
        vec![descriptor::<T>()]
    }
    fn read<'a>(r: &'a Run, s: &ScopeId, k: &[Raw]) -> Result<&'a T, Signal> {
        Ok(r.core.resolve(s, &r.id(k[0]))?)
    }
}
macro_rules! inputs {($($t:ident:$i:tt),+)=>{
    impl<$($t:Data),+> InputSpec for ($($t,)+){
        type Borrowed<'a>=($(&'a $t,)+);
        fn types()->Vec<(TypeId,&'static str)>{vec![$(descriptor::<$t>()),+]}
        fn read<'a>(r:&'a Run,s:&ScopeId,k:&[Raw])->Result<Self::Borrowed<'a>,Signal>{Ok(($(r.core.resolve::<$t>(s,&r.id(k[$i]))?,)+))}
    }
}}
inputs!(A:0,B:1);
inputs!(A:0,B:1,C:2);
inputs!(A:0,B:1,C:2,D:3);
inputs!(A:0,B:1,C:2,D:3,E:4);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14);
inputs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14,P:15);

pub trait RefShape: Copy + 'static {
    type Spec: InputSpec;
    type Owned: 'static;
    type Collected: RefShape;
    #[doc(hidden)]
    fn raw(self) -> Vec<Raw>;
    #[doc(hidden)]
    fn from_raw(r: &mut impl Iterator<Item = Raw>) -> Self;
    #[doc(hidden)]
    fn decode(v: &mut impl Iterator<Item = Box<dyn Any>>) -> Self::Owned;
    #[doc(hidden)]
    fn collectors(
        r: &mut Run,
        s: &ScopeId,
    ) -> Result<Vec<crate::core::identity::CollectorId>, Signal>;
    #[doc(hidden)]
    fn fresh(f: &mut Flow<'_>) -> Self {
        let k: Vec<_> = Self::Spec::types()
            .into_iter()
            .map(|(t, n)| f.allocate(t, n))
            .collect();
        Self::from_raw(&mut k.into_iter())
    }
    #[doc(hidden)]
    fn local(self, f: &mut Flow<'_>) -> Self {
        let k: Vec<_> = self.raw().into_iter().map(|r| f.ensure(r)).collect();
        Self::from_raw(&mut k.into_iter())
    }
}
impl RefShape for () {
    type Spec = ();
    type Owned = ();
    type Collected = ();
    fn raw(self) -> Vec<Raw> {
        vec![]
    }
    fn from_raw(_: &mut impl Iterator<Item = Raw>) {}
    fn decode(_: &mut impl Iterator<Item = Box<dyn Any>>) {}
    fn collectors(
        _: &mut Run,
        _: &ScopeId,
    ) -> Result<Vec<crate::core::identity::CollectorId>, Signal> {
        Ok(vec![])
    }
}
impl<T: Data> RefShape for Ref<T> {
    type Spec = T;
    type Owned = T;
    type Collected = Ref<Vec<T>>;
    fn raw(self) -> Vec<Raw> {
        vec![self.raw]
    }
    fn from_raw(r: &mut impl Iterator<Item = Raw>) -> Self {
        Self::from_raw(r.next().unwrap())
    }
    fn decode(v: &mut impl Iterator<Item = Box<dyn Any>>) -> T {
        *v.next()
            .unwrap()
            .downcast::<T>()
            .expect("Root type preflight")
    }
    fn collectors(
        r: &mut Run,
        s: &ScopeId,
    ) -> Result<Vec<crate::core::identity::CollectorId>, Signal> {
        Ok(vec![r.core.begin_collector::<T>(s)?])
    }
}
macro_rules! refs {($($t:ident:$i:tt),+)=>{
    impl<$($t:Data),+> RefShape for ($(Ref<$t>,)+){
        type Spec=($($t,)+);type Owned=($($t,)+);type Collected=($(Ref<Vec<$t>>,)+);
        fn raw(self)->Vec<Raw>{vec![$(self.$i.raw),+]}
        fn from_raw(r:&mut impl Iterator<Item=Raw>)->Self{($( {let _:PhantomData<$t>=PhantomData;Ref::from_raw(r.next().unwrap())},)+)}
        fn decode(v:&mut impl Iterator<Item=Box<dyn Any>>)->Self::Owned{($(*v.next().unwrap().downcast::<$t>().expect("Root type preflight"),)+)}
        fn collectors(r:&mut Run,s:&ScopeId)->Result<Vec<crate::core::identity::CollectorId>,Signal>{Ok(vec![$(r.core.begin_collector::<$t>(s)?),+])}
    }
}}
refs!(A:0,B:1);
refs!(A:0,B:1,C:2);
refs!(A:0,B:1,C:2,D:3);
refs!(A:0,B:1,C:2,D:3,E:4);
refs!(A:0,B:1,C:2,D:3,E:4,F:5);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14);
refs!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14,P:15);
pub trait RootInput: InputSpec {
    type Refs: RefShape;
    #[doc(hidden)]
    fn values(self) -> Vec<(&'static str, Box<dyn Any>)>;
}
impl RootInput for () {
    type Refs = ();
    fn values(self) -> Vec<(&'static str, Box<dyn Any>)> {
        vec![]
    }
}
impl<T: Data> RootInput for T {
    type Refs = Ref<T>;
    fn values(self) -> Vec<(&'static str, Box<dyn Any>)> {
        vec![(type_name::<T>(), Box::new(self))]
    }
}
macro_rules! roots {($($t:ident:$i:tt),+)=>{
    impl<$($t:Data),+> RootInput for ($($t,)+){type Refs=($(Ref<$t>,)+);
        fn values(self)->Vec<(&'static str,Box<dyn Any>)>{vec![$((type_name::<$t>(),Box::new(self.$i) as Box<dyn Any>),)+]}
    }
}}
roots!(A:0,B:1);
roots!(A:0,B:1,C:2);
roots!(A:0,B:1,C:2,D:3);
roots!(A:0,B:1,C:2,D:3,E:4);
roots!(A:0,B:1,C:2,D:3,E:4,F:5);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14);
roots!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14,P:15);
#[doc(hidden)]
pub trait StateShape: RefShape {
    fn register(self, r: &mut Run, s: &ScopeId) -> Result<Vec<ControlStateId>, Signal>;
    fn import(self, r: &Run, states: &[ControlStateId]) -> Vec<StateImportSlot>;
}
impl<T: Data> StateShape for Ref<T> {
    fn register(self, r: &mut Run, s: &ScopeId) -> Result<Vec<ControlStateId>, Signal> {
        Ok(vec![r.core.register_state::<T>(s, &r.id(self.raw))?])
    }
    fn import(self, r: &Run, states: &[ControlStateId]) -> Vec<StateImportSlot> {
        vec![StateImportSlot::new::<T>(&states[0], &r.id(self.raw))]
    }
}
macro_rules! states {($($t:ident:$i:tt),+)=>{
    impl<$($t:Data),+> StateShape for ($(Ref<$t>,)+){
        fn register(self,r:&mut Run,s:&ScopeId)->Result<Vec<ControlStateId>,Signal>{Ok(vec![$(r.core.register_state::<$t>(s,&r.id(self.$i.raw))?),+])}
        fn import(self,r:&Run,states:&[ControlStateId])->Vec<StateImportSlot>{vec![$(StateImportSlot::new::<$t>(&states[$i],&r.id(self.$i.raw))),+]}
    }
}}
states!(A:0,B:1);
states!(A:0,B:1,C:2);
states!(A:0,B:1,C:2,D:3);
states!(A:0,B:1,C:2,D:3,E:4);
states!(A:0,B:1,C:2,D:3,E:4,F:5);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14);
states!(A:0,B:1,C:2,D:3,E:4,F:5,G:6,H:7,I:8,J:9,K:10,L:11,M:12,N:13,O:14,P:15);
pub trait NodeOutput: 'static {
    type Refs: RefShape<Owned = Self>;
    #[doc(hidden)]
    fn store(self, r: &mut Run, s: &ScopeId, k: Self::Refs) -> Result<(), Signal>;
}
impl NodeOutput for () {
    type Refs = ();
    fn store(self, _: &mut Run, _: &ScopeId, _: ()) -> Result<(), Signal> {
        Ok(())
    }
}
impl<T: Data> NodeOutput for T {
    type Refs = Ref<T>;
    fn store(self, r: &mut Run, s: &ScopeId, k: Ref<T>) -> Result<(), Signal> {
        r.core.register_owned(s, &r.id(k.raw), self)?;
        Ok(())
    }
}
pub trait Node {
    type Input: InputSpec;
    type Output: NodeOutput;
    async fn run(&self, q: Query<&Self::Input>) -> Result<Self::Output, BodyError>;
}
impl<N: Node + ?Sized> Node for &N {
    type Input = N::Input;
    type Output = N::Output;
    async fn run(&self, q: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        (**self).run(q).await
    }
}
impl<N: Node> Node for Arc<N> {
    type Input = N::Input;
    type Output = N::Output;
    async fn run(&self, q: Query<&Self::Input>) -> Result<Self::Output, BodyError> {
        (**self).run(q).await
    }
}
#[doc(hidden)]
pub struct FunctionCall<O>(PhantomData<O>);
#[doc(hidden)]
pub struct StructCall;
pub trait Callable<A: RefShape, K> {
    type Output: NodeOutput;
    fn call<'a>(
        &'a self,
        q: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
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
        q: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<O, BodyError>> + 'a>> {
        Box::pin(async move { self(q).await })
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
        q: Query<<A::Spec as InputSpec>::Borrowed<'a>>,
    ) -> Pin<Box<dyn Future<Output = Result<Self::Output, BodyError>> + 'a>> {
        Box::pin(async move { self.run(Query::<&<N as Node>::Input>::new(q.get())).await })
    }
}
