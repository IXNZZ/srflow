//! 接线签名：输出分类、声明端口、Marker 与构建期错误。
//!
//! 本模块只描述**构建期**元数据：逻辑位置、类型标记与输出分类。它不持有业务值、
//! 不接触 Execution，也不建立公开 API。
//!
//! 分类模型（V21-05 §4.1／§4.2）：
//! - [`Data<O>`](Data) 表示一份 owned 业务 Data 输出；
//! - [`Unit`](Unit) 表示显式无业务输出的 Signature（不创建 unit Data）；
//! - [`Out2<O1, O2>`](Out2) 表示 Orchestrator 的两个异构输出位置。
//!
//! 接线 Marker（[`Wiring`] 的实现）把"调用对象类别 + 输入类型 tuple + 输出分类"编成
//! 一个类型参数，使同一个 `then` 能在不要求调用方标注 Marker 的前提下推导出返回形态。
//! 每个 Marker 都必须在 trait 参数里携带**输入与输出**类型：只携带输入 tuple 会让
//! 输出类型成为未约束参数（`error[E0207]`），这是硬约束而不是风格选择。

use std::any::TypeId;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

use super::context::BodyError;
use super::data_ref::DataRef;
use super::internal_error::InternalError;
use super::ref_id::{RefId, RefIdAllocator};

/// 已接线调用返回的 boxed Future：生命周期绑定当前调用（Context／Node／输入借用），
/// 不要求 `Send`，也不强求输入借用是 `'static`。
pub(crate) type NodeFut<'a, O> = Pin<Box<dyn Future<Output = Result<O, BodyError>> + 'a>>;

/// 一个已声明位置的元数据：位置、声明类型与诊断用类型名。
#[derive(Debug, Clone)]
pub(crate) struct DeclaredPort {
    position: RefId,
    expected: TypeId,
    expected_name: &'static str,
}

#[allow(dead_code)] // 声明元数据由 Definition 构建期记录，V21-05 的驱动按位置使用
impl DeclaredPort {
    /// 以 `T` 声明位置的类型。
    pub(crate) fn new<T: 'static>(position: RefId) -> Self {
        Self {
            position,
            expected: TypeId::of::<T>(),
            expected_name: std::any::type_name::<T>(),
        }
    }

    /// 以运行时类型声明位置（Orchestrator 的 child-local 端口由内部 Definition 决定）。
    pub(crate) fn with_type(
        position: RefId,
        expected: TypeId,
        expected_name: &'static str,
    ) -> Self {
        Self {
            position,
            expected,
            expected_name,
        }
    }

    /// 逻辑位置。
    pub(crate) fn position(&self) -> &RefId {
        &self.position
    }

    /// 声明类型。
    pub(crate) fn expected(&self) -> TypeId {
        self.expected
    }

    /// 声明类型的名字。
    pub(crate) fn expected_name(&self) -> &'static str {
        self.expected_name
    }
}

/// 输出分类：决定声明端口数量、`then` 的返回形态与登记方式。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait OutKind: 'static {
    /// 业务执行体返回的价值类型（`Unit` 为 `()`）。
    type Output: 'static;

    /// `then` 的构建输出（调用方看到的形态）。
    type BuildOutput;
    /// 整组分配得到的类型化位置槽。
    type Slots;

    /// 声明端口数量。unit 输出为 0，不产生 unit Ref 分配。
    const DECLARED: usize;

    /// 声明端口的类型名与 `TypeId`（位置在整组分配后填充）。
    fn port_types() -> Vec<(&'static str, TypeId)>;

    /// checked 整组分配本次调用的输出位置。
    fn allocate(allocator: &RefIdAllocator) -> Result<Self::Slots, BuildError>;

    /// 已分配位置的列表（按声明顺序）。
    fn positions(slots: &Self::Slots) -> Vec<RefId>;

    /// 从位置槽组装构建输出（分配成功后的路径，不含可失败操作）。
    fn assemble(slots: Self::Slots) -> Self::BuildOutput;
}

/// 一份 owned 业务 Data 输出。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Data<O>(PhantomData<fn() -> O>);
/// 显式无业务输出的 Signature。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Unit;
/// Orchestrator 的两个异构输出位置。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct Out2<O1, O2>(PhantomData<fn() -> (O1, O2)>);

/// 0／1／2 个位置的类型化槽。
#[derive(Debug)]
pub(crate) struct Slots0;
#[derive(Debug)]
pub(crate) struct Slots1(RefId);
#[derive(Debug)]
pub(crate) struct Slots2(RefId, RefId);

impl<O: 'static> OutKind for Data<O> {
    type Output = O;
    type BuildOutput = DataRef<O>;
    type Slots = Slots1;
    const DECLARED: usize = 1;

    fn port_types() -> Vec<(&'static str, TypeId)> {
        vec![(std::any::type_name::<O>(), TypeId::of::<O>())]
    }

    fn allocate(allocator: &RefIdAllocator) -> Result<Self::Slots, BuildError> {
        let mut batch = allocator.allocate_batch(1)?;
        Ok(Slots1(batch.pop().expect("checked batch of one position")))
    }

    fn positions(slots: &Self::Slots) -> Vec<RefId> {
        vec![slots.0.clone()]
    }

    fn assemble(slots: Self::Slots) -> Self::BuildOutput {
        DataRef::from_position(slots.0)
    }
}

impl OutKind for Unit {
    type Output = ();
    type BuildOutput = ();
    type Slots = Slots0;
    const DECLARED: usize = 0;

    fn port_types() -> Vec<(&'static str, TypeId)> {
        Vec::new()
    }

    fn allocate(_allocator: &RefIdAllocator) -> Result<Self::Slots, BuildError> {
        Ok(Slots0)
    }

    fn positions(_slots: &Self::Slots) -> Vec<RefId> {
        Vec::new()
    }

    fn assemble(_slots: Self::Slots) -> Self::BuildOutput {}
}

impl<O1: 'static, O2: 'static> OutKind for Out2<O1, O2> {
    type Output = ();
    type BuildOutput = (DataRef<O1>, DataRef<O2>);
    type Slots = Slots2;
    const DECLARED: usize = 2;

    fn port_types() -> Vec<(&'static str, TypeId)> {
        vec![
            (std::any::type_name::<O1>(), TypeId::of::<O1>()),
            (std::any::type_name::<O2>(), TypeId::of::<O2>()),
        ]
    }

    fn allocate(allocator: &RefIdAllocator) -> Result<Self::Slots, BuildError> {
        let mut batch = allocator.allocate_batch(2)?;
        let second = batch.pop().expect("checked batch of two positions");
        let first = batch.pop().expect("checked batch of two positions");
        Ok(Slots2(first, second))
    }

    fn positions(slots: &Self::Slots) -> Vec<RefId> {
        vec![slots.0.clone(), slots.1.clone()]
    }

    fn assemble(slots: Self::Slots) -> Self::BuildOutput {
        (
            DataRef::from_position(slots.0),
            DataRef::from_position(slots.1),
        )
    }
}

/// 接线 Marker：普通同步函数 item。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct SyncFnSig<I, K>(PhantomData<fn() -> (I, K)>);
/// 接线 Marker：普通异步函数 item。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct AsyncFnSig<I, K>(PhantomData<fn() -> (I, K)>);
/// 接线 Marker：按值持有的具体结构体 Node。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct NodeSig<I, K>(PhantomData<fn() -> (I, K)>);
/// 接线 Marker：`Arc<具体 Node>` 句柄。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct ArcNodeSig<I, K>(PhantomData<fn() -> (I, K)>);
/// 接线 Marker：Orchestrator。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) struct OrchSig<I, K>(PhantomData<fn() -> (I, K)>);

/// Marker → 输出槽、构建输出与声明端口。
///
/// 每种 Marker 家族只有一份实现，全部委托给输出分类 [`OutKind`]；Marker 只负责分类，
/// 不参与运行时执行。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait Wiring: 'static {
    type Slots;
    type BuildOutput;

    /// 声明端口数量。
    const DECLARED: usize;
    /// 是否为"普通函数 item"路径：该路径只支持产生 Data，输出为 `()` 时构建期拒绝。
    const ORDINARY_FUNCTION: bool;

    /// 该 Marker 是否走 Orchestrator 调用边界（`OrchSig`）：用于报告 branch 包装的真实调用类别。
    const ORCHESTRATOR: bool;

    /// 业务输出类型的 `TypeId`（`Unit` 为 `()`）。
    fn output_type() -> TypeId;

    /// 声明端口的类型名与 `TypeId`（位置在整组分配后填充）。
    fn port_types() -> Vec<(&'static str, TypeId)>;

    /// checked 整组分配。
    fn allocate(allocator: &RefIdAllocator) -> Result<Self::Slots, BuildError>;

    /// 已分配位置列表。
    fn positions(slots: &Self::Slots) -> Vec<RefId>;

    /// 组装构建输出。
    fn assemble(slots: Self::Slots) -> Self::BuildOutput;
}

macro_rules! wiring_for_marker {
    ($marker:ident, $ordinary:expr, $orchestrator:expr) => {
        impl<I: 'static, K: OutKind> Wiring for $marker<I, K> {
            type Slots = K::Slots;
            type BuildOutput = K::BuildOutput;
            const DECLARED: usize = K::DECLARED;
            const ORDINARY_FUNCTION: bool = $ordinary;
            const ORCHESTRATOR: bool = $orchestrator;

            fn output_type() -> TypeId {
                TypeId::of::<K::Output>()
            }

            fn port_types() -> Vec<(&'static str, TypeId)> {
                K::port_types()
            }

            fn allocate(allocator: &RefIdAllocator) -> Result<Self::Slots, BuildError> {
                K::allocate(allocator)
            }

            fn positions(slots: &Self::Slots) -> Vec<RefId> {
                K::positions(slots)
            }

            fn assemble(slots: Self::Slots) -> Self::BuildOutput {
                K::assemble(slots)
            }
        }
    };
}

wiring_for_marker!(SyncFnSig, true, false);
wiring_for_marker!(AsyncFnSig, true, false);
wiring_for_marker!(NodeSig, false, false);
wiring_for_marker!(ArcNodeSig, false, false);
wiring_for_marker!(OrchSig, false, true);

/// 正式输入 Signature 的类型清单：把 Orchestrator 的 `I` 与内部声明输入逐项可比。
///
/// 它使 `then` 能在输出分配与 Step 追加之前核对"接线 Signature ↔ 内部 Definition 声明"
/// 两者一致，并把不一致报成 [`BuildError::SignatureMismatch`]（含输入序号），而不是拖到
/// 执行期 Import 才失败。
pub(crate) trait InputTypes {
    /// 每个输入位置的类型名与 `TypeId`（按声明顺序）。
    fn input_types() -> Vec<(&'static str, TypeId)>;
}

impl InputTypes for () {
    fn input_types() -> Vec<(&'static str, TypeId)> {
        Vec::new()
    }
}

impl<A: 'static> InputTypes for (A,) {
    fn input_types() -> Vec<(&'static str, TypeId)> {
        vec![(std::any::type_name::<A>(), TypeId::of::<A>())]
    }
}

impl<A: 'static, B: 'static> InputTypes for (A, B) {
    fn input_types() -> Vec<(&'static str, TypeId)> {
        vec![
            (std::any::type_name::<A>(), TypeId::of::<A>()),
            (std::any::type_name::<B>(), TypeId::of::<B>()),
        ]
    }
}

/// `then` 的接线参数形态：`()`、`DataRef<A>`、`(DataRef<A>, DataRef<B>)`。
///
/// 一个业务 tuple Data 的 `DataRef<(A, B)>` 与两个独立位置的
/// `(DataRef<A>, DataRef<B>)` 是不同形态，不会被自动拆分或合并。
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) trait WireInputs {
    /// 本次接线使用的逻辑输入位置（按书写顺序）。
    fn positions(&self) -> Vec<&RefId>;
}

impl WireInputs for () {
    fn positions(&self) -> Vec<&RefId> {
        Vec::new()
    }
}

impl<A> WireInputs for DataRef<A> {
    fn positions(&self) -> Vec<&RefId> {
        vec![self.position()]
    }
}

impl<A, B> WireInputs for (DataRef<A>, DataRef<B>) {
    fn positions(&self) -> Vec<&RefId> {
        vec![self.0.position(), self.1.position()]
    }
}

/// 构建期拒绝：接线与 Signature 的错误都在追加 Step 与分配输出位置之前发生。
///
/// 这不是公开错误 API；执行期错误另由 [`BodyError`] 表达。
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)] // V21-06 接入完整 Flow／Root 驱动前，V21-05 的真实执行样本是唯一消费者
pub(crate) enum BuildError {
    /// 普通函数 item 的输出类型为 `()`：本次支持范围只允许产生 Data。
    ///
    /// 该实例化在类型层可以成立（`O = ()`），因此必须在构建预检按 `TypeId` 拒绝，
    /// 而不是以编译歧义代替支持结论。
    UnsupportedFunctionUnitOutput,
    /// 正式协议声明了一份 Data 输出，但业务类型为 `()`：不得据此当作 unit 输出。
    UnitDataOutputNotSupported,
    /// 输入位置来自另一条 Definition 来源序列。
    ForeignPosition(RefId),
    /// 位置同源，但未登记为当前 Definition 的输入或此前 Step 的输出。
    ///
    /// 同时用于输入接线与 Flow 完成时的输出选择：两处拒绝的都是"该位置不是本 Definition
    /// 的合法来源"。
    UndeclaredPosition(RefId),
    /// 完成 Flow 的输出选择与该位置的真实声明类型不一致。
    ///
    /// 位置的声明类型来自已声明输入或已产出 Step 的输出端口；选择类型由
    /// `FlowOutput` 决定。内部 `DataRef::from_position` 可以给已有位置套上任意类型，
    /// 因此完成前必须比对元数据，不能只找到"某个已声明位置"。
    OutputTypeMismatch {
        position: RefId,
        expected: &'static str,
        actual: &'static str,
    },
    /// 完成 Flow 的输出选择里同一位置出现多次（含 clone 同一个 `DataRef` 后再选择）。
    ///
    /// 与 [`Self::UndeclaredPosition`] 区分：位置本身合法，只是被重复选择；判重依据完整
    /// `RefId` 身份，不按裸序号或 Data 类型。输出端口 likewise 不允许同一位置重复声明。
    DuplicateOutputPosition(RefId),
    /// 声明端口数量与传入参数数量不一致。
    InputCountMismatch { expected: usize, supplied: usize },
    /// 输入位置已声明类型与实际传入类型不一致。
    InputTypeMismatch {
        position: RefId,
        expected: &'static str,
        actual: &'static str,
    },
    /// Orchestrator 内部 Definition 的声明输入与接线 Signature 不一致。
    SignatureMismatch {
        index: usize,
        expected: &'static str,
        actual: &'static str,
    },
    /// 同一个 branch key 被登记两次。
    ///
    /// 只报告"key 类别"，不要求 `R: Debug`／`Display`；比较按登记 key 的共享借用进行。
    DuplicateBranchKey,
    /// 已经登记过 default，不能再登记第二个。
    SecondDefault,
    /// 登记的 branch 输出数量与本次完成的共同 Output Signature 不一致。
    ///
    /// 结构上 branch 与 Match 共用同一个 `K`，本拒绝是完成装配的防御性整组校验。
    BranchOutputArity {
        /// 登记顺序下标。
        branch: usize,
        /// 共同 Signature 的端口数量。
        expected: usize,
        /// 该 branch 声明的端口数量。
        supplied: usize,
    },
    /// 登记的 branch 输出类型与本次完成的共同 Output Signature 不一致。
    BranchOutputType {
        /// 登记顺序下标。
        branch: usize,
        /// 声明顺序下标。
        position: usize,
        /// 共同 Signature 的类型名。
        expected: &'static str,
        /// 该 branch 声明的类型名。
        actual: &'static str,
    },
    /// `RefId` 序号空间耗尽：整组分配失败，本次调用不消耗任何序号。
    OutputPositionExhausted,
}

impl BuildError {
    /// 稳定的说明文本，供交接记录与断言引用。
    pub(crate) fn note(&self) -> &'static str {
        match self {
            Self::UnsupportedFunctionUnitOutput => "ordinary function unit output is unsupported",
            Self::UnitDataOutputNotSupported => {
                "a data output declaration may not carry the unit type"
            }
            Self::ForeignPosition(_) => "input position belongs to another definition",
            Self::UndeclaredPosition(_) => "position is not declared for this definition",
            Self::OutputTypeMismatch { .. } => {
                "output selection type does not match the declared position"
            }
            Self::DuplicateOutputPosition(_) => "output position is selected more than once",
            Self::InputCountMismatch { .. } => "input count does not match the declared signature",
            Self::InputTypeMismatch { .. } => "input type does not match the declared position",
            Self::SignatureMismatch { .. } => {
                "orchestrator definition inputs do not match its wiring signature"
            }
            Self::DuplicateBranchKey => "a branch key is already registered",
            Self::SecondDefault => "a match default is already registered",
            Self::BranchOutputArity { .. } => {
                "a registered branch output count does not match the common output signature"
            }
            Self::BranchOutputType { .. } => {
                "a registered branch output type does not match the common output signature"
            }
            Self::OutputPositionExhausted => "ref id sequence space exhausted",
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignPosition(position) => write!(f, "{}: {position}", self.note()),
            Self::UndeclaredPosition(position) => write!(f, "{}: {position}", self.note()),
            Self::OutputTypeMismatch {
                position,
                expected,
                actual,
            } => write!(
                f,
                "{}: {position} declares `{expected}`, completion selects `{actual}`",
                self.note()
            ),
            Self::DuplicateOutputPosition(position) => write!(f, "{}: {position}", self.note()),
            Self::InputCountMismatch { expected, supplied } => {
                write!(
                    f,
                    "{}: expected {expected}, supplied {supplied}",
                    self.note()
                )
            }
            Self::InputTypeMismatch {
                position,
                expected,
                actual,
            } => write!(
                f,
                "{}: {position} declares `{expected}`, wiring supplies `{actual}`",
                self.note()
            ),
            Self::SignatureMismatch {
                index,
                expected,
                actual,
            } => write!(
                f,
                "{}: input {index} declares `{expected}`, signature supplies `{actual}`",
                self.note()
            ),
            Self::BranchOutputArity {
                branch,
                expected,
                supplied,
            } => write!(
                f,
                "{}: the common signature declares {expected} port(s), branch {branch} declares {supplied}",
                self.note()
            ),
            Self::BranchOutputType {
                branch,
                position,
                expected,
                actual,
            } => write!(
                f,
                "{}: the common signature declares `{expected}` at output {position}, branch {branch} declares `{actual}`",
                self.note()
            ),
            Self::UnsupportedFunctionUnitOutput
            | Self::UnitDataOutputNotSupported
            | Self::DuplicateBranchKey
            | Self::SecondDefault
            | Self::OutputPositionExhausted => f.write_str(self.note()),
        }
    }
}

impl From<InternalError> for BuildError {
    fn from(_source: InternalError) -> Self {
        // 构建期唯一可耗尽的序列是 `RefId`；其它存储错误不会出现在无业务值的构建期。
        Self::OutputPositionExhausted
    }
}

#[cfg(test)]
mod tests {
    use super::super::ref_id::RefIdSource;
    use super::*;

    #[test]
    fn v21_05_out_kinds_declare_expected_port_counts() {
        assert_eq!(<Data<u32> as OutKind>::DECLARED, 1);
        assert_eq!(<Unit as OutKind>::DECLARED, 0);
        assert_eq!(<Out2<u32, u64> as OutKind>::DECLARED, 2);
        assert_eq!(
            <NodeSig<(u32,), Data<String>> as Wiring>::port_types().len(),
            1
        );
        assert!(<Unit as OutKind>::positions(&Slots0).is_empty());
    }

    #[test]
    fn v21_05_batch_allocation_is_atomic_on_exhaustion() {
        let source = RefIdSource::with_start(u64::MAX - 1);
        let allocator = RefIdAllocator::new(source);
        let error = <Out2<u32, u64> as OutKind>::allocate(&allocator).unwrap_err();
        assert_eq!(error, BuildError::OutputPositionExhausted);
        // 整组失败不消耗序号：仍然只有一个序号可用。
        assert_eq!(allocator.next_probe(), u64::MAX - 1);
        assert_eq!(allocator.allocate_batch(1).unwrap()[0].seq(), u64::MAX - 1);
        assert!(allocator.allocate_batch(1).is_err());
    }

    #[test]
    fn v21_05_unit_output_allocates_nothing() {
        let source = RefIdSource::new();
        let allocator = RefIdAllocator::new(source);
        let slots = <Unit as OutKind>::allocate(&allocator).unwrap();
        assert!(<Unit as OutKind>::positions(&slots).is_empty());
        assert_eq!(allocator.next_probe(), 0);
    }
}
