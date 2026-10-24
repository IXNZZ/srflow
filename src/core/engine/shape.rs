//! Data／Shape 的类型映射：`InputSpec`、`RefShape`、`Ref` 与 `Query`（SPEC §3、§5、§6）。
//!
//! 公开面提供声明与接线所需的类型映射；标为 `doc(hidden)` 的方法只服务框架内部
//! 登记与执行桥，不构成使用者协议。业务侧只写 `()`、`&T`、`&` 引用 tuple。

use crate::core::context::ExecutionContext;
use crate::core::engine::data::Data;
use crate::core::engine::error::ExecFailure;
use crate::core::engine::identity::PositionHandle;
use crate::core::identity::{CollectorId, ScopeId};
use crate::core::internal_error::ScopeError;
use crate::core::ref_id::RefId;
use crate::core::scope::{ControlStateId, StateImportSlot};
use std::any::{Any, TypeId};
use std::marker::PhantomData;

/// 声明槽位的静态类型描述；只用于诊断与来源校验，不参与值传递。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TypeDesc {
    pub(crate) id: TypeId,
    pub(crate) name: &'static str,
}

impl TypeDesc {
    /// 取得 `T` 的类型描述。
    pub(crate) fn of<T: 'static>() -> Self {
        Self {
            id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    /// 声明类型的 `TypeId`。
    pub(crate) fn type_id(self) -> TypeId {
        self.id
    }

    /// 声明类型名。
    pub(crate) fn type_name(self) -> &'static str {
        self.name
    }
}

impl std::fmt::Display for TypeDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

/// 一个声明槽位的逻辑位置与类型。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct PositionDesc {
    pub(crate) handle: PositionHandle,
    pub(crate) type_desc: TypeDesc,
}

/// `Query<T>` 的类型参数标记：把声明写成 `()`、`&T` 或引用 tuple。
#[doc(hidden)]
pub trait QueryType {
    /// `get` 返回的视图类型：单输入为 `&T`，tuple 输入为各叶子引用 tuple。
    type View;
}

impl QueryType for () {
    type View = ();
}

impl<'a, T: InputSpec> QueryType for &'a T {
    type View = T::Borrowed<'a>;
}

/// 本次 Node invocation 的只读输入视图。
///
/// `Query<&(A, B)>` 的视图是 `(&A, &B)`；`Query<&A>` 的视图是 `&A`；`Query<()>`
/// 的视图是 `()`。不制造 owned 输入 tuple，也不向业务暴露 Ref／Context／Container。
pub struct Query<T: QueryType> {
    view: T::View,
    marker: PhantomData<fn() -> T>,
}

impl<T: QueryType> Query<T> {
    /// 由只读借用视图构造（crate 内部调用桥使用）。
    pub(crate) fn from_view(view: T::View) -> Self {
        Self {
            view,
            marker: PhantomData,
        }
    }

    /// 取得声明输入的只读借用。
    pub fn get(self) -> T::View {
        self.view
    }
}

/// 借用来源：由框架在真实调用前构造（crate 内部）。
#[derive(Clone, Copy)]
pub(crate) struct BorrowSource<'a> {
    pub(crate) ctx: &'a ExecutionContext,
    pub(crate) scope: &'a ScopeId,
    pub(crate) positions: &'a [RefId],
}

/// Node／Root 输入声明的数据形状：`()`、单 Data 或 2～16 个 flat Data tuple。
pub trait InputSpec: 'static {
    /// 运行期只读借用视图；单输入为 `&T`，tuple 输入为各叶子引用 tuple。
    type Borrowed<'a>: QueryType<View = Self::Borrowed<'a>>;
}

impl InputSpec for () {
    type Borrowed<'a> = ();
}

impl<T: Data> InputSpec for T {
    type Borrowed<'a> = &'a T;
}

/// 输入形状的登记与借用操作（crate 内部协议，使用者不实现）。
pub(crate) trait InputOps: InputSpec {
    /// 槽位顺序的类型描述。
    fn type_descs() -> Vec<TypeDesc>;

    /// 从当前 invocation 的声明位置取得只读借用。
    fn read<'a>(source: BorrowSource<'a>) -> Result<Self::Borrowed<'a>, ExecFailure>;
}

impl InputOps for () {
    fn type_descs() -> Vec<TypeDesc> {
        Vec::new()
    }

    fn read<'a>(_: BorrowSource<'a>) -> Result<Self::Borrowed<'a>, ExecFailure> {
        Ok(())
    }
}

impl<T: Data> InputOps for T {
    fn type_descs() -> Vec<TypeDesc> {
        vec![TypeDesc::of::<T>()]
    }

    fn read<'a>(source: BorrowSource<'a>) -> Result<Self::Borrowed<'a>, ExecFailure> {
        borrow::<T>(source, 0)
    }
}

/// 从借用来源的第 `index` 个位置取得 `&T`；越界属于内部不变量破坏。
fn borrow<'a, T: Any>(source: BorrowSource<'a>, index: usize) -> Result<&'a T, ExecFailure> {
    let position = source
        .positions
        .get(index)
        .expect("input slot index within the declared shape");
    source
        .ctx
        .resolve::<T>(source.scope, position)
        .map_err(|error| ExecFailure::runtime("node input borrow rejected", &error))
}

/// 强类型逻辑依赖身份；Copy，不拥有、读取或复制 T。
///
/// 复制仍是同一逻辑位置；同类型的不同输入／Node 输出保持不同身份。Ref 不提供
/// 读值、字段投影或运行期身份访问器，也不产生 `Ref<()>`（`T: Data` 已排除 unit）。
pub struct Ref<T: Data> {
    handle: PositionHandle,
    marker: PhantomData<fn() -> T>,
}

impl<T: Data> Copy for Ref<T> {}

impl<T: Data> Clone for Ref<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Data> std::fmt::Debug for Ref<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ref")
            .field("definition", &self.handle.definition)
            .field("position", &self.handle.position)
            .finish()
    }
}

impl<T: Data> Ref<T> {
    /// 由位置句柄构造（crate 内部构建路径）；句柄不是来源证明，使用仍须通过 Definition 验证。
    pub(crate) fn from_handle(handle: PositionHandle) -> Self {
        Self {
            handle,
            marker: PhantomData,
        }
    }

    /// 逻辑位置句柄（crate 内部验证与结构检查使用，不读业务值）。
    #[allow(dead_code)] // 由定向测试、UI 夹具与结构检查使用
    pub(crate) fn handle(self) -> PositionHandle {
        self.handle
    }
}

/// 编排输出形状：`()`、单 Ref 或 2～16 个 flat Ref tuple。
pub trait RefShape: Copy + 'static {
    /// 对应的数据形状。
    type Spec: InputSpec;
    /// Root 成功交付的应用侧 owned 形状。
    type Owned: 'static;
}

/// 形状的登记、重建与 owned 组装操作（crate 内部协议，使用者不实现）。
pub(crate) trait ShapeOps: RefShape {
    /// 槽位顺序的类型描述；不读取业务值。
    fn type_descs() -> Vec<TypeDesc>;

    /// 槽位顺序的逻辑位置与类型。
    fn positions(self) -> Vec<PositionDesc>;

    /// 由已登记的位置重建形状（框架内部构建路径）。
    fn from_positions(handles: &mut dyn Iterator<Item = PositionHandle>) -> Self;

    /// 由按声明顺序取出的 owned 值组装应用侧结果（Root 提取收口）。
    fn from_owned(values: &mut dyn Iterator<Item = Box<dyn Any>>) -> Self::Owned;
}

/// 取得形状的下一个位置句柄；数量不匹配属于内部不变量破坏。
#[doc(hidden)]
pub fn next_handle(handles: &mut dyn Iterator<Item = PositionHandle>) -> PositionHandle {
    handles
        .next()
        .expect("RefShape slot count does not match the number of positions")
}

/// 取得形状的下一个 owned 值并还原为 `T`。
fn next_owned<T: 'static>(values: &mut dyn Iterator<Item = Box<dyn Any>>) -> T {
    *values
        .next()
        .expect("Root extraction supplies one value per declared slot")
        .downcast::<T>()
        .expect("Root extraction preflight guarantees the declared type")
}

impl RefShape for () {
    type Spec = ();
    type Owned = ();
}

impl ShapeOps for () {
    fn type_descs() -> Vec<TypeDesc> {
        Vec::new()
    }

    fn positions(self) -> Vec<PositionDesc> {
        Vec::new()
    }

    fn from_positions(_: &mut dyn Iterator<Item = PositionHandle>) {}

    fn from_owned(_: &mut dyn Iterator<Item = Box<dyn Any>>) {}
}

impl<T: Data> RefShape for Ref<T> {
    type Spec = T;
    type Owned = T;
}

impl<T: Data> ShapeOps for Ref<T> {
    fn type_descs() -> Vec<TypeDesc> {
        vec![TypeDesc::of::<T>()]
    }

    fn positions(self) -> Vec<PositionDesc> {
        vec![PositionDesc {
            handle: self.handle,
            type_desc: TypeDesc::of::<T>(),
        }]
    }

    fn from_positions(handles: &mut dyn Iterator<Item = PositionHandle>) -> Self {
        Ref::from_handle(next_handle(handles))
    }

    fn from_owned(values: &mut dyn Iterator<Item = Box<dyn Any>>) -> T {
        next_owned::<T>(values)
    }
}

/// Each 的 item Body 输出形状 → caller 侧聚合输出形状（SPEC §8）。
///
/// `()` 保持 `()`（零槽，不建 collector）；单 `Ref<O>` 聚合为 `Ref<Vec<O>>`；2～16 个
/// flat `Ref` tuple 逐槽聚合为对应的 `Ref<Vec<..>>`。同类型的不同逻辑位置仍是各自独立
/// 的聚合槽，不按类型合并。本 trait 属于公开消费面：调用者无需命名它，即可经
/// [`Flow::each`](crate::Flow::each) 推断结果并交给后续 Node／Chain／Root。
pub trait EachShape: RefShape {
    /// Each 在 caller 中的输出形状。
    type Collected: RefShape;
}

/// Each 的类型化 collector 桥（crate 内部协议，使用者不实现）。
///
/// 让 boundary 侧在不知道 item Body 输出类型、也不靠 `Any` 下转型决定控制结构的前提下，
/// 逐槽建立 typed 空 collector 并在全部项成功后把它们 finish 成 Each-owned 普通 `Vec`
/// Data。`O` 不上浮到通用 runner；允许与 [`ShapeOps::from_positions`] 一致的内部类型擦除
/// （每个槽的 `begin_collector::<A>` 在实例化期确定，运行期只持有 `CollectorId`）。
pub(crate) trait EachCollect: EachShape {
    /// 在 Each Scope 下按槽顺序建立空的 typed collector（unit 为零个）。
    fn begin_collectors(
        ctx: &mut ExecutionContext,
        each: &ScopeId,
    ) -> Result<Vec<CollectorId>, ScopeError>;

    /// 全部项成功后逐槽 finish：建构值成为新的普通 `Vec<A>` Data，并绑定到 Each 本地
    /// 最终输出位置（调用方保证 `targets` 与本次 `collectors` 逐槽对应）。
    fn finish_collectors(
        ctx: &mut ExecutionContext,
        each: &ScopeId,
        collectors: &[CollectorId],
        targets: &[RefId],
    ) -> Result<(), ScopeError> {
        if collectors.len() != targets.len() {
            return Err(ScopeError::Invariant {
                violated: "each collector group length does not match the declared slots",
            });
        }
        for (collector, target) in collectors.iter().zip(targets.iter()) {
            ctx.finish_collector(each, collector, target)?;
        }
        Ok(())
    }
}

impl EachShape for () {
    type Collected = ();
}

impl EachCollect for () {
    fn begin_collectors(
        _ctx: &mut ExecutionContext,
        _each: &ScopeId,
    ) -> Result<Vec<CollectorId>, ScopeError> {
        Ok(Vec::new())
    }
}

impl<T: Data> EachShape for Ref<T> {
    type Collected = Ref<Vec<T>>;
}

impl<T: Data> EachCollect for Ref<T> {
    fn begin_collectors(
        ctx: &mut ExecutionContext,
        each: &ScopeId,
    ) -> Result<Vec<CollectorId>, ScopeError> {
        ctx.begin_collector::<T>(each)
            .map(|collector| vec![collector])
    }
}

/// Each 的形式 item 输入绑定：`Ref<Vec<T>>` 的第 `index` 个元素作为受限借用绑定到
/// item Body 的正式位置（typed 桥，供 boundary 侧调用；不移动集合元素、不 Clone）。
pub(crate) fn each_bind_item<T: Data>(
    ctx: &mut ExecutionContext,
    item: &ScopeId,
    caller: &ScopeId,
    source: &RefId,
    target: &RefId,
    index: usize,
) -> Result<(), ScopeError> {
    ctx.bind_item_input::<T>(item, caller, source, target, index)
}

/// Each 的集合长度读取：只读取 `Ref<Vec<T>>` 的元素个数，借用立即结束，不跨可变
/// Context 操作（typed 桥，供 boundary 侧调用）。
pub(crate) fn each_collection_len<T: Data>(
    ctx: &ExecutionContext,
    scope: &ScopeId,
    position: &RefId,
) -> Result<usize, ScopeError> {
    ctx.resolve::<Vec<T>>(scope, position)
        .map(|items| items.len())
}

/// Iter 的状态形状（SPEC §3.2）：单 `Ref` 或 2～16 个 flat `Ref` tuple。
///
/// 它是 crate 内部协议，使用者不实现；`()` 刻意不实现它，因此无状态 Iter 不通过类型
/// 检查。状态槽只描述逻辑位置，不新增 `Clone`／`Copy`／`Send`／`Sync` 约束，也不复制
/// 业务值。
pub(crate) trait StateShape: ShapeOps {
    /// 在控制器 Scope 上逐槽登记初始状态；顺序与 Shape 的槽位顺序一致。
    ///
    /// `initial` 是各槽在控制器 Scope 中已绑定的本地引用；Imported 初始状态保持原 owner。
    fn register_states(
        ctx: &mut ExecutionContext,
        controller: &ScopeId,
        initial: &[RefId],
    ) -> Result<Vec<ControlStateId>, ScopeError>;

    /// 逐槽构造 StateImport 槽：控制器状态 → 本轮 formal current 位置。
    fn state_import_slots(states: &[ControlStateId], targets: &[RefId]) -> Vec<StateImportSlot>;
}

impl<T: Data> StateShape for Ref<T> {
    fn register_states(
        ctx: &mut ExecutionContext,
        controller: &ScopeId,
        initial: &[RefId],
    ) -> Result<Vec<ControlStateId>, ScopeError> {
        let local = initial
            .first()
            .expect("a single state slot supplies exactly one initial position");
        ctx.register_state::<T>(controller, local)
            .map(|state| vec![state])
    }

    fn state_import_slots(states: &[ControlStateId], targets: &[RefId]) -> Vec<StateImportSlot> {
        vec![StateImportSlot::new::<T>(
            states
                .first()
                .expect("a single state slot has exactly one control state"),
            targets
                .first()
                .expect("a single state slot has exactly one formal current position"),
        )]
    }
}

/// 为每个承诺的 arity 生成状态形状：逐槽类型化登记状态与构造 StateImport 槽。
macro_rules! tuple_state_shapes {
    ($($t:ident:$i:tt),+) => {
        impl<$($t: Data),+> StateShape for ($(Ref<$t>,)+) {
            fn register_states(
                ctx: &mut ExecutionContext,
                controller: &ScopeId,
                initial: &[RefId],
            ) -> Result<Vec<ControlStateId>, ScopeError> {
                Ok(vec![$(ctx.register_state::<$t>(controller, &initial[$i])?),+])
            }

            fn state_import_slots(
                states: &[ControlStateId],
                targets: &[RefId],
            ) -> Vec<StateImportSlot> {
                vec![$(StateImportSlot::new::<$t>(&states[$i], &targets[$i])),+]
            }
        }
    };
}

tuple_state_shapes!(A:0, B:1);
tuple_state_shapes!(A:0, B:1, C:2);
tuple_state_shapes!(A:0, B:1, C:2, D:3);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12);
tuple_state_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13);
tuple_state_shapes!(
    A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14
);
tuple_state_shapes!(
    A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14, P:15
);

/// Root 输入的类型映射：`()`、单 Data 或 2～16 个 flat Data tuple。
pub trait RootInput: InputSpec {
    /// Definition 构建闭包接收的逻辑 Ref 形状。
    type Refs: RefShape<Spec = Self>;
    /// 应用侧传入的 owned 输入形状。
    type Owned: 'static;
}

impl RootInput for () {
    type Refs = ();
    type Owned = ();
}

impl<T: Data> RootInput for T {
    type Refs = Ref<T>;
    type Owned = T;
}

/// Root 输入登记操作（crate 内部协议，使用者不实现）。
pub(crate) trait RootInputOps: RootInput {
    /// 按声明顺序交出 `(真实类型名, 擦除值)`；tuple 包装本身不是业务 Data。
    fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)>;
}

impl RootInputOps for () {
    fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)> {
        Vec::new()
    }
}

impl<T: Data> RootInputOps for T {
    fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)> {
        vec![(std::any::type_name::<T>(), Box::new(self))]
    }
}

/// Node 输出声明：单个 owned Data 或 `()`；tuple 不默认成为 Data 叶子。
pub trait NodeOutput: 'static {
    /// 登记到当前 Scope 输出位置对应的 Ref 形状。
    type Refs: RefShape;
}

impl NodeOutput for () {
    type Refs = ();
}

impl<T: Data> NodeOutput for T {
    type Refs = Ref<T>;
}

/// 输出登记操作（crate 内部协议，使用者不实现）。
pub(crate) trait OutputOps: NodeOutput {
    /// 把本次 owned 输出登记到声明位置；unit 不登记业务槽。
    fn store(
        self,
        ctx: &mut ExecutionContext,
        scope: &ScopeId,
        outputs: &[RefId],
    ) -> Result<(), ExecFailure>;
}

impl OutputOps for () {
    fn store(
        self,
        _ctx: &mut ExecutionContext,
        _scope: &ScopeId,
        _outputs: &[RefId],
    ) -> Result<(), ExecFailure> {
        Ok(())
    }
}

impl<T: Data> OutputOps for T {
    fn store(
        self,
        ctx: &mut ExecutionContext,
        scope: &ScopeId,
        outputs: &[RefId],
    ) -> Result<(), ExecFailure> {
        let position = outputs
            .first()
            .expect("a single Data output owns exactly one declared position");
        ctx.register_owned(scope, position, self)
            .map(|_| ())
            .map_err(|error| ExecFailure::runtime("node output registration rejected", &error))
    }
}

/// 为每个承诺 arity 生成 tuple 形状的声明映射：
/// QueryType（引用 tuple 的视图）、InputSpec（数据 tuple）、RefShape（Ref tuple + owned
/// 值 tuple）与 RootInput（Data tuple → Ref tuple）。
macro_rules! tuple_shapes {
    ($($t:ident:$i:tt),+) => {
        impl<$($t),+> QueryType for ($($t,)+) {
            type View = Self;
        }

        impl<$($t: Data),+> InputSpec for ($($t,)+) {
            type Borrowed<'a> = ($(&'a $t,)+);
        }

        impl<$($t: Data),+> InputOps for ($($t,)+) {
            fn type_descs() -> Vec<TypeDesc> {
                vec![$(TypeDesc::of::<$t>()),+]
            }

            fn read<'a>(source: BorrowSource<'a>) -> Result<Self::Borrowed<'a>, ExecFailure> {
                Ok(($(borrow::<$t>(source, $i)?,)+))
            }
        }

        impl<$($t: Data),+> RefShape for ($(Ref<$t>,)+) {
            type Spec = ($($t,)+);
            type Owned = ($($t,)+);
        }

        impl<$($t: Data),+> ShapeOps for ($(Ref<$t>,)+) {
            fn type_descs() -> Vec<TypeDesc> {
                vec![$(TypeDesc::of::<$t>()),+]
            }

            fn positions(self) -> Vec<PositionDesc> {
                vec![$(PositionDesc {
                    handle: self.$i.handle,
                    type_desc: TypeDesc::of::<$t>(),
                }),+]
            }

            fn from_positions(handles: &mut dyn Iterator<Item = PositionHandle>) -> Self {
                ($(Ref::<$t>::from_handle(next_handle(handles)),)+)
            }

            fn from_owned(values: &mut dyn Iterator<Item = Box<dyn Any>>) -> Self::Owned {
                ($(next_owned::<$t>(values),)+)
            }
        }

        impl<$($t: Data),+> EachShape for ($(Ref<$t>,)+) {
            type Collected = ($(Ref<Vec<$t>>,)+);
        }

        impl<$($t: Data),+> EachCollect for ($(Ref<$t>,)+) {
            fn begin_collectors(
                ctx: &mut ExecutionContext,
                each: &ScopeId,
            ) -> Result<Vec<CollectorId>, ScopeError> {
                Ok(vec![$(ctx.begin_collector::<$t>(each)?,)+])
            }
        }

        impl<$($t: Data),+> RootInput for ($($t,)+) {
            type Refs = ($(Ref<$t>,)+);
            type Owned = ($($t,)+);
        }

        impl<$($t: Data),+> RootInputOps for ($($t,)+) {
            fn into_values(self) -> Vec<(&'static str, Box<dyn Any>)> {
                vec![$((std::any::type_name::<$t>(), Box::new(self.$i) as Box<dyn Any>)),+]
            }
        }
    };
}

tuple_shapes!(A:0, B:1);
tuple_shapes!(A:0, B:1, C:2);
tuple_shapes!(A:0, B:1, C:2, D:3);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13);
tuple_shapes!(A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14);
tuple_shapes!(
    A:0, B:1, C:2, D:3, E:4, F:5, G:6, H:7, I:8, J:9, K:10, L:11, M:12, N:13, O:14, P:15
);

#[cfg(test)]
#[path = "../../../tests/internal/engine/shape_each_shape_tests.rs"]
mod each_shape_tests;
