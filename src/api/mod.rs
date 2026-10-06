//! 公开 façade：构建器、Root 入口与公开错误映射。
//!
//! 内部实现仍在 `crate::core`。本模块只做两件事：把内部构建器的可失败入口包装成
//! 公开错误类型，并把泛型分派收进 crate 内实现的 operation trait（调用者只看到
//! 公开 bound 与公开返回值）。不新增流程语义、控制策略或运行保证。

use crate::core::data_ref::DataRef;
use crate::core::each::{Each, EachShape};
use crate::core::flow::{Flow, FlowInputs};
use crate::core::loop_orchestrator::{Loop, LoopShape};
use crate::core::match_orchestrator::Match;
use crate::core::runtime::RootError;

// ------------------------------------------------------------------ 构建错误

/// Definition 构建错误：接线与 Signature 的错误在追加 Step 与分配输出位置之前返回。
///
/// 变体与内部构建错误一一对应；内部位置身份（`RefId`）不进入公开 payload，位置类
/// 拒绝不携带参数。`Display` 输出稳定说明；类型名来自编译期声明。
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildError {
    /// 普通函数 item 的输出类型为 `()`：普通函数只支持产生 Data。
    ///
    /// 该实例化在类型层可以成立（`O = ()`），因此在 Definition 构建预检按 `TypeId` 拒绝。
    UnsupportedFunctionUnitOutput,
    /// 正式协议声明了一份 Data 输出，但业务类型为 `()`：不得据此当作 unit 输出。
    UnitDataOutputNotSupported,
    /// 输入位置来自另一条 Definition 来源序列。
    ForeignPosition,
    /// 位置同源，但未登记为当前 Definition 的输入或此前 Step 的输出。
    UndeclaredPosition,
    /// 完成 Flow 的输出选择与该位置的真实声明类型不一致。
    OutputTypeMismatch {
        /// 位置声明类型名。
        expected: &'static str,
        /// 本次选择类型名。
        actual: &'static str,
    },
    /// 完成 Flow 的输出选择里同一位置出现多次。
    /// 输出端口 likewise 不允许同一位置重复声明。
    DuplicateOutputPosition,
    /// 声明端口数量与传入参数数量不一致。
    InputCountMismatch {
        /// 声明端口数量。
        expected: usize,
        /// 实际提供数量。
        supplied: usize,
    },
    /// 输入位置已声明类型与实际传入类型不一致。
    InputTypeMismatch {
        /// 位置声明类型名。
        expected: &'static str,
        /// 实际传入类型名。
        actual: &'static str,
    },
    /// Orchestrator 内部 Definition 的声明输入与接线 Signature 不一致。
    SignatureMismatch {
        /// 不一致的输入序号。
        index: usize,
        /// Signature 类型名。
        expected: &'static str,
        /// 内部声明类型名。
        actual: &'static str,
    },
    /// 同一个 branch key 被登记两次。
    DuplicateBranchKey,
    /// 已经登记过 default，不能再登记第二个。
    SecondDefault,
    /// 已经登记过 Each body，不能再登记第二个。
    SecondEachBody,
    /// 完成 Each 之前必须登记唯一 body。
    EachBodyMissing,
    /// 已经登记过 Loop body，不能再登记第二个。
    SecondLoopBody,
    /// 完成 Loop 之前必须登记唯一 body。
    LoopBodyMissing,
    /// 登记包装的形状不满足 Loop 约束（恰好一个 Step、恰好一个声明输出）。
    LoopWrapperShape,
    /// 登记的 branch 输出数量与本次完成的共同 Output Signature 不一致。
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
    /// Root Signature 的声明输出端口与 `K` 的数量或类型不一致。
    RootOutputSignatureMismatch {
        /// 不一致的声明序号。
        index: usize,
        /// `K` 声明的类型名（或缺端口标记）。
        expected: &'static str,
        /// Definition 声明端口的类型名（或缺端口标记）。
        actual: &'static str,
    },
}

impl BuildError {
    /// 稳定说明文本；与 `Display` 的前缀一致。
    pub fn message(&self) -> &'static str {
        match self {
            Self::UnsupportedFunctionUnitOutput => "ordinary function unit output is unsupported",
            Self::UnitDataOutputNotSupported => {
                "a data output declaration may not carry the unit type"
            }
            Self::ForeignPosition => "input position belongs to another definition",
            Self::UndeclaredPosition => "position is not declared for this definition",
            Self::OutputTypeMismatch { .. } => {
                "output selection type does not match the declared position"
            }
            Self::DuplicateOutputPosition => "output position is selected more than once",
            Self::InputCountMismatch { .. } => "input count does not match the declared signature",
            Self::InputTypeMismatch { .. } => "input type does not match the declared position",
            Self::SignatureMismatch { .. } => {
                "orchestrator definition inputs do not match its wiring signature"
            }
            Self::DuplicateBranchKey => "a branch key is already registered",
            Self::SecondDefault => "a match default is already registered",
            Self::SecondEachBody => "an each body is already registered",
            Self::EachBodyMissing => "an each body must be registered before finish",
            Self::SecondLoopBody => "a loop body is already registered",
            Self::LoopBodyMissing => "a loop body must be registered before finish",
            Self::LoopWrapperShape => {
                "a loop body wrapper must declare exactly one step and one output"
            }
            Self::BranchOutputArity { .. } => {
                "a registered branch output count does not match the common output signature"
            }
            Self::BranchOutputType { .. } => {
                "a registered branch output type does not match the common output signature"
            }
            Self::OutputPositionExhausted => "ref id sequence space exhausted",
            Self::RootOutputSignatureMismatch { .. } => {
                "root output ports do not match the root output signature"
            }
        }
    }

    /// 从内部构建错误转换；内部位置身份不进入公开 payload。
    pub(crate) fn from_internal(source: crate::core::signature::BuildError) -> Self {
        use crate::core::signature::BuildError as Internal;
        match source {
            Internal::UnsupportedFunctionUnitOutput => Self::UnsupportedFunctionUnitOutput,
            Internal::UnitDataOutputNotSupported => Self::UnitDataOutputNotSupported,
            Internal::ForeignPosition(_) => Self::ForeignPosition,
            Internal::UndeclaredPosition(_) => Self::UndeclaredPosition,
            Internal::OutputTypeMismatch {
                expected, actual, ..
            } => Self::OutputTypeMismatch { expected, actual },
            Internal::DuplicateOutputPosition(_) => Self::DuplicateOutputPosition,
            Internal::InputCountMismatch { expected, supplied } => {
                Self::InputCountMismatch { expected, supplied }
            }
            Internal::InputTypeMismatch {
                expected, actual, ..
            } => Self::InputTypeMismatch { expected, actual },
            Internal::SignatureMismatch {
                index,
                expected,
                actual,
            } => Self::SignatureMismatch {
                index,
                expected,
                actual,
            },
            Internal::DuplicateBranchKey => Self::DuplicateBranchKey,
            Internal::SecondDefault => Self::SecondDefault,
            Internal::SecondEachBody => Self::SecondEachBody,
            Internal::EachBodyMissing => Self::EachBodyMissing,
            Internal::SecondLoopBody => Self::SecondLoopBody,
            Internal::LoopBodyMissing => Self::LoopBodyMissing,
            Internal::LoopWrapperShape => Self::LoopWrapperShape,
            Internal::BranchOutputArity {
                branch,
                expected,
                supplied,
            } => Self::BranchOutputArity {
                branch,
                expected,
                supplied,
            },
            Internal::BranchOutputType {
                branch,
                position,
                expected,
                actual,
            } => Self::BranchOutputType {
                branch,
                position,
                expected,
                actual,
            },
            Internal::OutputPositionExhausted => Self::OutputPositionExhausted,
            Internal::RootOutputSignatureMismatch {
                index,
                expected,
                actual,
            } => Self::RootOutputSignatureMismatch {
                index,
                expected,
                actual,
            },
        }
    }
}

impl std::fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputTypeMismatch { expected, actual } => write!(
                formatter,
                "{}: position declares `{expected}`, completion selects `{actual}`",
                self.message()
            ),
            Self::InputCountMismatch { expected, supplied } => write!(
                formatter,
                "{}: expected {expected}, supplied {supplied}",
                self.message()
            ),
            Self::InputTypeMismatch { expected, actual } => write!(
                formatter,
                "{}: position declares `{expected}`, wiring supplies `{actual}`",
                self.message()
            ),
            Self::SignatureMismatch {
                index,
                expected,
                actual,
            } => write!(
                formatter,
                "{}: input {index} declares `{expected}`, signature supplies `{actual}`",
                self.message()
            ),
            Self::BranchOutputArity {
                branch,
                expected,
                supplied,
            } => write!(
                formatter,
                "{}: the common signature declares {expected} port(s), branch {branch} declares {supplied}",
                self.message()
            ),
            Self::BranchOutputType {
                branch,
                position,
                expected,
                actual,
            } => write!(
                formatter,
                "{}: the common signature declares `{expected}` at output {position}, branch {branch} declares `{actual}`",
                self.message()
            ),
            Self::RootOutputSignatureMismatch {
                index,
                expected,
                actual,
            } => write!(
                formatter,
                "{}: port {index} expects `{expected}`, definition declares `{actual}`",
                self.message()
            ),
            _ => formatter.write_str(self.message()),
        }
    }
}

impl std::error::Error for BuildError {}

// ------------------------------------------------------------------ 执行错误

/// Root 执行失败的阶段：区分构建／装配、body、提取预检与关闭。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunErrorStage {
    /// 输入／输出 Signature 与真实 Definition 不一致；业务体不执行。
    Signature,
    /// Root 输入登记失败；业务体不执行。
    Assembly,
    /// Root body（含 child 调用）执行失败或取消终止。
    Body,
    /// Root 提取预检拒绝：任何 take 之前整体拒绝。
    Preflight,
    /// Root 提取后的关闭失败（清理未完成；不覆盖更早的失败）。
    Close,
}

/// Root 失败的公开分类：可区分业务终止、预检拒绝与框架阶段失败。
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunErrorKind {
    /// Signature 阶段失败：Definition 与调用处形状不一致。
    SignatureMismatch,
    /// Root 输入登记失败。
    InputRegistrationFailed,
    /// 业务体失败或取消终止（说明见 [`RunError::business_note`]）。
    BusinessTerminated,
    /// 输出预检发现两个声明位置指向同一份数据。
    DuplicateRootDataId,
    /// 输出预检的其他拒绝（类型、存活、责任或声明不符）。
    OutputPreflightRejected,
    /// 提取后的关闭失败。
    CloseFailed,
}

/// Root 执行失败。
///
/// 保留阶段、分类与可观察诊断；内部身份（`ScopeId`／`RefId`／`DataId`）与内部错误
/// 类型不进入公开访问面。业务 Node 返回的说明经 [`RunError::business_note`] 可读。
pub struct RunError {
    inner: RootError,
}

impl RunError {
    /// 失败发生的阶段。
    pub fn stage(&self) -> RunErrorStage {
        use crate::core::runtime::RootErrorStage as Stage;
        match self.inner.stage() {
            Stage::Signature => RunErrorStage::Signature,
            Stage::Assembly => RunErrorStage::Assembly,
            Stage::Body => RunErrorStage::Body,
            Stage::Preflight => RunErrorStage::Preflight,
            Stage::Close => RunErrorStage::Close,
        }
    }

    /// 公开分类。
    pub fn kind(&self) -> RunErrorKind {
        use crate::core::internal_error::ScopeError;
        use crate::core::runtime::RootErrorStage as Stage;
        match self.inner.stage() {
            Stage::Signature => RunErrorKind::SignatureMismatch,
            Stage::Assembly => RunErrorKind::InputRegistrationFailed,
            Stage::Body => RunErrorKind::BusinessTerminated,
            Stage::Preflight => match self.inner.scope_error() {
                Some(ScopeError::DuplicateRootDataId { .. }) => RunErrorKind::DuplicateRootDataId,
                _ => RunErrorKind::OutputPreflightRejected,
            },
            Stage::Close => RunErrorKind::CloseFailed,
        }
    }

    /// 稳定的阶段说明文本。
    pub fn message(&self) -> &'static str {
        self.inner.note()
    }

    /// Signature 阶段的构建错误（如有）。
    pub fn build_error(&self) -> Option<BuildError> {
        self.inner
            .build_error()
            .map(|build| BuildError::from_internal(build.clone()))
    }

    /// 业务体失败时实际保存的说明（仅 Body 阶段；其它阶段返回 `None`）。
    pub fn business_note(&self) -> Option<&'static str> {
        if matches!(self.stage(), RunErrorStage::Body) {
            self.inner.termination_note()
        } else {
            None
        }
    }

    /// 装配失败时已成功登记的输入数（业务体未执行）。
    pub fn registered_inputs(&self) -> usize {
        self.inner.registered_inputs()
    }

    /// 是否存在首次清理失败诊断（与阶段诊断互不覆盖）。
    pub fn cleanup_failed(&self) -> bool {
        self.inner.cleanup_failure().is_some()
    }

    /// 是否存在提取后的关闭失败诊断。
    pub fn close_failed(&self) -> bool {
        self.inner.close_error().is_some()
    }

    /// 从内部 Root 错误转换。
    pub(crate) fn from_internal(inner: RootError) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunError")
            .field("stage", &self.stage())
            .field("kind", &self.kind())
            .field("message", &self.message())
            .field("business_note", &self.business_note())
            .field("cleanup_failed", &self.cleanup_failed())
            .field("close_failed", &self.close_failed())
            .finish()
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{:?}: {} ({:?})",
            self.stage(),
            self.message(),
            self.kind()
        )?;
        if let Some(note) = self.business_note()
            && note != self.message()
        {
            write!(formatter, ": {note}")?;
        }
        Ok(())
    }
}

impl std::error::Error for RunError {}

// ------------------------------------------------------------------ Flow 构建器

/// 单输入 Flow 构建器：类型参数直接表示唯一输入的业务类型。
///
/// 未完成的 Builder 既不能被执行，也不能作为 child 接线；只有 [`FlowBuilder::finish`]
/// 产出 [`Flow`] 之后才成立。
pub struct FlowBuilder<I: 'static> {
    inner: crate::core::flow::FlowBuilder<(I,)>,
}

impl<I: 'static> FlowBuilder<I> {
    /// 建立单输入 Flow，并返回该输入位置的句柄。
    pub fn start() -> Result<(Self, DataRef<I>), BuildError>
    where
        Self: FlowStartOp<Handles = DataRef<I>>,
    {
        Self::start_op()
    }

    /// 追加一个调用点：参数形态与输出形态由编译期类型检查，构建期拒绝另行报告。
    ///
    /// `callable` 可以是普通同步／异步函数、实现 [`crate::NodeCall1`] 等协议的具体
    /// 结构体、`Arc<具体 Node>`，或完成态 [`Flow`]／[`Match`]／[`Each`]／[`Loop`]。
    /// `args` 是 [`DataRef<T>`] 或两个 [`DataRef`] 的 tuple；`()` 只用于零输入调用。
    pub fn then<C, M, A0>(
        &mut self,
        callable: C,
        args: A0,
    ) -> Result<<Self as ThenOp<C, M, A0>>::BuildOutput, BuildError>
    where
        Self: ThenOp<C, M, A0>,
    {
        self.then_op(callable, args)
    }

    /// 完成 Flow：整组校验输出选择，再声明输出端口并形成不可变完成态。
    ///
    /// `choice` 按输出分类给出：`()`（[`crate::Unit`]）、[`DataRef<O>`](DataRef)
    /// 或两个 [`DataRef`] 的 tuple（`Out2`）。失败不产生 Flow、不留半份输出声明。
    pub fn finish<K, C>(self, choice: C) -> Result<<Self as FlowFinishOp<K, C>>::Output, BuildError>
    where
        Self: FlowFinishOp<K, C>,
    {
        self.finish_op(choice)
    }
}

/// 单输入构建器起始协议：由 crate 实现，调用方只调用 [`FlowBuilder::start`]。
pub trait FlowStartOp {
    /// 单输入位置句柄。
    type Handles;

    /// 由 [`FlowBuilder::start`] 调用；调用者不直接调用。
    fn start_op() -> Result<(Self, Self::Handles), BuildError>
    where
        Self: Sized;
}

impl<A: 'static> FlowStartOp for FlowBuilder<A>
where
    (A,): crate::core::flow::FlowInputsDeclare + FlowInputs<Handles = DataRef<A>>,
{
    type Handles = DataRef<A>;

    fn start_op() -> Result<(Self, Self::Handles), BuildError> {
        let (inner, handles) =
            crate::core::flow::FlowBuilder::<(A,)>::start().map_err(BuildError::from_internal)?;
        Ok((Self { inner }, handles))
    }
}

/// 双输入 Flow 构建器：两个输入类型作为独立泛型参数。
pub struct FlowBuilder2<A: 'static, B: 'static> {
    inner: crate::core::flow::FlowBuilder<(A, B)>,
}

impl<A: 'static, B: 'static> FlowBuilder2<A, B> {
    /// 建立双输入 Flow，并返回两个输入位置的句柄。
    pub fn start() -> Result<(Self, <Self as FlowStart2Op>::Handles), BuildError>
    where
        Self: FlowStart2Op<Handles = (DataRef<A>, DataRef<B>)>,
    {
        Self::start_op()
    }

    /// 追加一个调用点：参数形态与输出形态由编译期类型检查，构建期拒绝另行报告。
    pub fn then<C, M, A0>(
        &mut self,
        callable: C,
        args: A0,
    ) -> Result<<Self as ThenOp<C, M, A0>>::BuildOutput, BuildError>
    where
        Self: ThenOp<C, M, A0>,
    {
        self.then_op(callable, args)
    }

    /// 完成 Flow：整组校验输出选择，再声明输出端口并形成不可变完成态。
    pub fn finish<K, C>(self, choice: C) -> Result<<Self as FlowFinishOp<K, C>>::Output, BuildError>
    where
        Self: FlowFinishOp<K, C>,
    {
        self.finish_op(choice)
    }
}

/// 双输入构建器起始协议：由 crate 实现，调用方只调用 [`FlowBuilder2::start`]。
pub trait FlowStart2Op {
    /// 双输入位置句柄。
    type Handles;

    /// 由 [`FlowBuilder2::start`] 调用；调用者不直接调用。
    fn start_op() -> Result<(Self, Self::Handles), BuildError>
    where
        Self: Sized;
}

impl<A: 'static, B: 'static> FlowStart2Op for FlowBuilder2<A, B>
where
    (A, B): crate::core::flow::FlowInputsDeclare + FlowInputs<Handles = (DataRef<A>, DataRef<B>)>,
{
    type Handles = (DataRef<A>, DataRef<B>);

    fn start_op() -> Result<(Self, Self::Handles), BuildError> {
        let (inner, handles) =
            crate::core::flow::FlowBuilder::<(A, B)>::start().map_err(BuildError::from_internal)?;
        Ok((Self { inner }, handles))
    }
}

/// 接线协议：由 crate 为受支持的调用对象实现；调用者只调用 [`FlowBuilder::then`]。
pub trait ThenOp<C, M, A0> {
    /// `then` 的构建输出：`DataRef<O>`、`()` 或两个 [`DataRef`] 的 tuple。
    type BuildOutput;

    /// 由 [`FlowBuilder::then`] 调用；调用者不直接调用。
    fn then_op(&mut self, callable: C, args: A0) -> Result<Self::BuildOutput, BuildError>;
}

impl<I, C, M, A0> ThenOp<C, M, A0> for FlowBuilder<I>
where
    I: 'static,
    C: crate::core::builder::BuildSite<M, A0>,
    M: crate::core::signature::Wiring,
    A0: crate::core::signature::WireInputs,
    C: crate::core::builder::IntoCallSite<M, A0, BuildOutput = M::BuildOutput>,
{
    type BuildOutput = C::BuildOutput;

    fn then_op(&mut self, callable: C, args: A0) -> Result<Self::BuildOutput, BuildError> {
        use crate::core::builder::TypedCallBuilder;
        self.inner
            .then(callable, args)
            .map_err(BuildError::from_internal)
    }
}

impl<A, B, C, M, A0> ThenOp<C, M, A0> for FlowBuilder2<A, B>
where
    A: 'static,
    B: 'static,
    C: crate::core::builder::BuildSite<M, A0>,
    M: crate::core::signature::Wiring,
    A0: crate::core::signature::WireInputs,
    C: crate::core::builder::IntoCallSite<M, A0, BuildOutput = M::BuildOutput>,
{
    type BuildOutput = C::BuildOutput;

    fn then_op(&mut self, callable: C, args: A0) -> Result<Self::BuildOutput, BuildError> {
        use crate::core::builder::TypedCallBuilder;
        self.inner
            .then(callable, args)
            .map_err(BuildError::from_internal)
    }
}

/// 完成协议：由 crate 为受支持的输出选择实现；调用者只调用 [`FlowBuilder::finish`]。
pub trait FlowFinishOp<K, C> {
    /// 完成结果（[`Flow<I, K>`](Flow)）。
    type Output;

    /// 由 [`FlowBuilder::finish`] 调用；调用者不直接调用。
    fn finish_op(self, choice: C) -> Result<Self::Output, BuildError>;
}

impl<I, K, C> FlowFinishOp<K, C> for FlowBuilder<I>
where
    I: 'static,
    K: crate::core::signature::OutKind,
    C: crate::core::flow::FlowOutput<K>,
{
    type Output = Flow<(I,), K>;

    fn finish_op(self, choice: C) -> Result<Self::Output, BuildError> {
        self.inner
            .finish::<K, C>(choice)
            .map_err(BuildError::from_internal)
    }
}

impl<A, B, K, C> FlowFinishOp<K, C> for FlowBuilder2<A, B>
where
    A: 'static,
    B: 'static,
    K: crate::core::signature::OutKind,
    C: crate::core::flow::FlowOutput<K>,
{
    type Output = Flow<(A, B), K>;

    fn finish_op(self, choice: C) -> Result<Self::Output, BuildError> {
        self.inner
            .finish::<K, C>(choice)
            .map_err(BuildError::from_internal)
    }
}

// ------------------------------------------------------------------ Match 构建器

/// Match 构建器：声明路由 `R` 与业务输入 `A`，登记 key／default，再完成共同输出。
///
/// key 是构建期由用户提供的 `R` 配置值；同一条 key 不能登记两次；default 最多一个。
/// 未选 branch 不创建 Scope、不执行业务。
pub struct MatchBuilder<R, A, K> {
    inner: crate::core::match_orchestrator::MatchBuilder<R, A, K>,
}

impl<R, A, K> MatchBuilder<R, A, K> {
    /// 建立 Match：声明路由与业务输入两个非空输入位置。
    pub fn start() -> Result<Self, BuildError>
    where
        Self: MatchStartOp,
    {
        Self::start_op()
    }

    /// 登记一个 key 命中 branch。
    pub fn branch<C, M>(&mut self, key: R, callable: C) -> Result<(), BuildError>
    where
        Self: MatchBranchOp<R, C, M>,
    {
        self.branch_op(key, callable)
    }

    /// 登记 default：只在未命中任何 key 时使用；最多一个。
    pub fn default<C, M>(&mut self, callable: C) -> Result<(), BuildError>
    where
        Self: MatchBranchOp<R, C, M>,
    {
        self.default_op(callable)
    }

    /// 完成 Match：整组校验各 branch 的共同输出，再一次性装配共同端口。
    pub fn finish(self) -> Result<Match<R, A, K>, BuildError>
    where
        Self: MatchFinishOp<Output = Match<R, A, K>>,
    {
        self.finish_op()
    }
}

/// Match 起始协议：由 crate 实现；调用者只调用 [`MatchBuilder::start`]。
pub trait MatchStartOp: Sized {
    /// 由 [`MatchBuilder::start`] 调用；调用者不直接调用。
    fn start_op() -> Result<Self, BuildError>;
}

impl<R: 'static + Eq, A: 'static, K: crate::core::signature::OutKind> MatchStartOp
    for MatchBuilder<R, A, K>
{
    fn start_op() -> Result<Self, BuildError> {
        let inner = crate::core::match_orchestrator::MatchBuilder::<R, A, K>::start()
            .map_err(BuildError::from_internal)?;
        Ok(Self { inner })
    }
}

/// Match 分支登记协议：由 crate 实现；调用者只调用 [`MatchBuilder::branch`]／
/// [`MatchBuilder::default`]。
pub trait MatchBranchOp<R, C, M> {
    /// 由 [`MatchBuilder::branch`] 调用；调用者不直接调用。
    fn branch_op(&mut self, key: R, callable: C) -> Result<(), BuildError>;

    /// 由 [`MatchBuilder::default`] 调用；调用者不直接调用。
    fn default_op(&mut self, callable: C) -> Result<(), BuildError>;
}

impl<R, A, K, C, M> MatchBranchOp<R, C, M> for MatchBuilder<R, A, K>
where
    R: 'static + Eq,
    A: 'static,
    K: crate::core::signature::OutKind,
    C: crate::core::builder::BuildSite<M, DataRef<A>>
        + crate::core::builder::IntoCallSite<M, DataRef<A>, BuildOutput = M::BuildOutput>,
    M: crate::core::signature::Wiring,
    M::BuildOutput: crate::core::flow::FlowOutput<K>,
{
    fn branch_op(&mut self, key: R, callable: C) -> Result<(), BuildError> {
        self.inner
            .branch::<C, M>(key, callable)
            .map_err(BuildError::from_internal)
    }

    fn default_op(&mut self, callable: C) -> Result<(), BuildError> {
        self.inner
            .default::<C, M>(callable)
            .map_err(BuildError::from_internal)
    }
}

/// Match 完成协议：由 crate 实现；调用者只调用 [`MatchBuilder::finish`]。
pub trait MatchFinishOp {
    /// 完成结果。
    type Output;

    /// 由 [`MatchBuilder::finish`] 调用；调用者不直接调用。
    fn finish_op(self) -> Result<Self::Output, BuildError>;
}

impl<R, A, K> MatchFinishOp for MatchBuilder<R, A, K>
where
    R: 'static + Eq,
    A: 'static,
    K: crate::core::signature::OutKind,
{
    type Output = Match<R, A, K>;

    fn finish_op(self) -> Result<Self::Output, BuildError> {
        self.inner.finish().map_err(BuildError::from_internal)
    }
}

// ------------------------------------------------------------------ Each 构建器

/// Each 构建器：声明集合（+ 可选 shared）输入，登记唯一 body，完成最终 `Vec<O>` 输出。
pub struct EachBuilder<Sh: EachShape, O: 'static> {
    inner: crate::core::each::EachBuilder<Sh, O>,
}

impl<Sh: EachShape, O: 'static> EachBuilder<Sh, O> {
    /// 建立 Each：声明输入与最终输出端口，并开始包装 body。
    ///
    /// 形状由类型标注选定：[`crate::EachOnly`]（无 shared）或 [`crate::EachShared`]
    /// （一个 shared Data）。
    pub fn start() -> Result<Self, BuildError>
    where
        Self: EachStartOp,
    {
        Self::start_op()
    }

    /// 登记 body：作为包装唯一 Step，输出必须是单份 `Data<O>`。
    ///
    /// 函数／结构体 Node／`Arc<具体 Node>`／完成态 [`Flow`] 都走同一接线；重复登记在
    /// 追加 Step 之前以 [`BuildError::SecondEachBody`] 拒绝。
    pub fn then_body<C, M>(&mut self, body: C) -> Result<(), BuildError>
    where
        Self: EachBodyOp<C, M>,
    {
        self.body_op(body)
    }

    /// 完成 Each：形成不可变完成态，并可作 Root 或 child 使用。
    pub fn finish(self) -> Result<Each<Sh, O>, BuildError> {
        self.inner.finish().map_err(BuildError::from_internal)
    }
}

/// Each 起始协议：由 crate 实现；调用者只调用 [`EachBuilder::start`]。
pub trait EachStartOp: Sized {
    /// 由 [`EachBuilder::start`] 调用；调用者不直接调用。
    fn start_op() -> Result<Self, BuildError>;
}

impl<Sh, O> EachStartOp for EachBuilder<Sh, O>
where
    Sh: EachShape + crate::core::each::EachShapeSpec,
    O: 'static,
    Sh::I: crate::core::each::EachInputs,
    Sh::Wrapper: crate::core::flow::FlowInputsDeclare,
{
    fn start_op() -> Result<Self, BuildError> {
        let inner =
            crate::core::each::EachBuilder::<Sh, O>::start().map_err(BuildError::from_internal)?;
        Ok(Self { inner })
    }
}

/// Each body 登记协议：由 crate 实现；调用者只调用 [`EachBuilder::then_body`]。
pub trait EachBodyOp<C, M> {
    /// 由 [`EachBuilder::then_body`] 调用；调用者不直接调用。
    fn body_op(&mut self, body: C) -> Result<(), BuildError>;
}

impl<Sh, O, C, M> EachBodyOp<C, M> for EachBuilder<Sh, O>
where
    Sh: crate::core::each::EachShapeSpec,
    O: 'static,
    C: crate::core::builder::BuildSite<M, <Sh::Wrapper as FlowInputs>::Handles>,
    M: crate::core::signature::Wiring<BuildOutput = DataRef<O>>,
    C: crate::core::builder::IntoCallSite<
            M,
            <Sh::Wrapper as FlowInputs>::Handles,
            BuildOutput = DataRef<O>,
        >,
    <Sh::Wrapper as FlowInputs>::Handles: crate::core::signature::WireInputs + Clone,
{
    fn body_op(&mut self, body: C) -> Result<(), BuildError> {
        self.inner
            .then_body::<C, M>(body)
            .map_err(BuildError::from_internal)
    }
}

// ------------------------------------------------------------------ Loop 构建器

/// Loop 构建器：声明形状输入，登记唯一 Round body，完成最终状态输出。
///
/// 形状由类型标注选定：[`crate::Retry1`]／[`crate::Retry2`]（每轮重新导入原始输入）
/// 或 [`crate::Iter1`]／[`crate::Iter2`]（当前状态经窄许可推进）。推进决定由被推进
/// 值类型实现 [`crate::LoopControl`] 表达。
pub struct LoopBuilder<Sh: LoopShape> {
    inner: crate::core::loop_orchestrator::LoopBuilder<Sh>,
}

impl<Sh: LoopShape> LoopBuilder<Sh> {
    /// 建立 Loop：声明形状输入与最终输出端口，并开始包装 Round body。
    pub fn start() -> Result<Self, BuildError>
    where
        Self: LoopStartOp,
    {
        Self::start_op()
    }

    /// 登记 body：作为包装唯一 Step，输出必须是单份 `Data<Sh::Value>`。
    pub fn then_body<C, M>(&mut self, body: C) -> Result<(), BuildError>
    where
        Self: LoopBodyOp<C, M>,
    {
        self.body_op(body)
    }

    /// 完成 Loop：形成不可变完成态，并可作 Root 或 child 使用。
    pub fn finish(self) -> Result<Loop<Sh>, BuildError> {
        self.inner.finish().map_err(BuildError::from_internal)
    }
}

/// Loop 起始协议：由 crate 实现；调用者只调用 [`LoopBuilder::start`]。
pub trait LoopStartOp: Sized {
    /// 由 [`LoopBuilder::start`] 调用；调用者不直接调用。
    fn start_op() -> Result<Self, BuildError>;
}

impl<Sh> LoopStartOp for LoopBuilder<Sh>
where
    Sh: LoopShape + crate::core::loop_orchestrator::LoopShapeSpec,
    Sh::I: crate::core::loop_orchestrator::LoopInputs,
    Sh::Wrapper: crate::core::flow::FlowInputsDeclare,
{
    fn start_op() -> Result<Self, BuildError> {
        let inner = crate::core::loop_orchestrator::LoopBuilder::<Sh>::start()
            .map_err(BuildError::from_internal)?;
        Ok(Self { inner })
    }
}

/// Loop body 登记协议：由 crate 实现；调用者只调用 [`LoopBuilder::then_body`]。
pub trait LoopBodyOp<C, M> {
    /// 由 [`LoopBuilder::then_body`] 调用；调用者不直接调用。
    fn body_op(&mut self, body: C) -> Result<(), BuildError>;
}

impl<Sh, C, M> LoopBodyOp<C, M> for LoopBuilder<Sh>
where
    Sh: crate::core::loop_orchestrator::LoopShapeSpec,
    C: crate::core::builder::BuildSite<M, <Sh::Wrapper as FlowInputs>::Handles>,
    M: crate::core::signature::Wiring<BuildOutput = DataRef<Sh::Value>>,
    C: crate::core::builder::IntoCallSite<
            M,
            <Sh::Wrapper as FlowInputs>::Handles,
            BuildOutput = DataRef<Sh::Value>,
        >,
    <Sh::Wrapper as FlowInputs>::Handles: crate::core::signature::WireInputs + Clone,
{
    fn body_op(&mut self, body: C) -> Result<(), BuildError> {
        self.inner
            .then_body::<C, M>(body)
            .map_err(BuildError::from_internal)
    }
}

// ------------------------------------------------------------------ Root 入口

/// Application 侧唯一的 Root 执行入口。
///
/// 无状态：不缓存上一次的 Context／输入／结果／终止状态；每次调用都创建新的
/// Execution、Context 与 RootScope。同一个完成态定义可以执行多次，每次独立身份空间。
/// Root 对象只被不可变借用（`&O`）；未被轮询时尚未开始执行，输入随 Future 一同销毁。
pub struct Runtime;

impl Runtime {
    /// 执行一个完成态 Root Orchestrator，成功时把声明输出全部移交 Application。
    ///
    /// 支持范围：单／双非空 owned 输入与 [`crate::Unit`]／[`crate::Data<O>`](crate::Data)
    /// ／[`crate::Out2<O1, O2>`](crate::Out2) 输出。顺序为"Signature 预检 → 登记输入 →
    /// 运行 body → 冻结／整组预检 → 同步 take 并解除 Root 责任 → 关闭 Root"；
    /// 任何一步失败都不返回部分 owned 输出。
    pub async fn execute<O, I, K>(
        root: &O,
        input: <Self as RunRoot<O, I, K>>::Input,
    ) -> Result<<Self as RunRoot<O, I, K>>::Owned, RunError>
    where
        Self: RunRoot<O, I, K>,
    {
        <Self as RunRoot<O, I, K>>::run_op(root, input).await
    }
}

/// Root 执行协议：由 crate 为受支持的 Root 形状实现；调用者只调用
/// [`Runtime::execute`]。
pub trait RunRoot<O, I, K> {
    /// 成功时交给 Application 的 owned 结果类型。
    type Owned;

    /// Application 侧输入形状：单输入直接传值，双输入传两个值组成的 tuple。
    type Input;

    /// 由 [`Runtime::execute`] 调用；调用者不直接调用。
    fn run_op(root: &O, input: Self::Input) -> impl Future<Output = Result<Self::Owned, RunError>>;
}

impl<O, I, K> RunRoot<O, I, K> for Runtime
where
    O: crate::core::orchestrator::OrchCall<I, K>,
    I: 'static + crate::core::signature::InputTypes + crate::core::root_signature::RootInputs<I>,
    K: crate::core::signature::OutKind + crate::core::root_signature::RootOutputs<K>,
{
    type Owned = <K as crate::core::root_signature::RootOutputs<K>>::Owned;
    type Input = <I as crate::core::root_signature::RootInputs<I>>::ApplicationInput;

    fn run_op(root: &O, input: Self::Input) -> impl Future<Output = Result<Self::Owned, RunError>> {
        run_root_facade::<O, I, K>(root, input)
    }
}

/// [`RunRoot::run_op`] 的实际执行体：内部 `execute` 的公开错误映射。
async fn run_root_facade<O, I, K>(
    root: &O,
    input: <I as crate::core::root_signature::RootInputs<I>>::ApplicationInput,
) -> Result<<K as crate::core::root_signature::RootOutputs<K>>::Owned, RunError>
where
    O: crate::core::orchestrator::OrchCall<I, K>,
    I: 'static + crate::core::signature::InputTypes + crate::core::root_signature::RootInputs<I>,
    K: crate::core::signature::OutKind + crate::core::root_signature::RootOutputs<K>,
{
    let input = <I as crate::core::root_signature::RootInputs<I>>::into_internal(input);
    crate::core::runtime::Runtime::execute::<O, I, K>(root, input)
        .await
        .map_err(RunError::from_internal)
}
