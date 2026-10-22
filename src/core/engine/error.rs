//! Node 错误通道与 Definition／Root 错误模型（SPEC §12、Engine §8）。
//!
//! 公开层：
//! - [`BodyError`]／[`ControlSignal`]／[`BodyFailure`]／[`RetryError`]／[`IterBreak`]：业务叶子与
//!   控制传播的数据模型，保留动态源错误与 source chain；
//! - [`BuildError`]／[`RunError`]：Definition 构建失败与 Root 执行结果。
//!
//! 内部层（`pub(crate)`）：位置级失败明细与执行链内部失败 [`ExecFailure`]。公开诊断只提供
//! `Display`／`Error::source`，不作为读取或操纵 DataId／Scope／Target 的入口。

use crate::core::engine::definition::{BodyRole, DeclarationKind, PositionSource};
use crate::core::engine::identity::{BodyId, DefinitionId, PositionId};
use crate::core::engine::shape::TypeDesc;
use crate::core::internal_error::ScopeError;
use std::error::Error;
use std::fmt;

// ---------------------------------------------------------------- 业务错误通道

/// Node 的错误通道：可捕获的 Control 或不可恢复 Failure。
#[derive(Debug)]
pub enum BodyError {
    /// 显式控制请求；沿调用链由最近的对应控制器处理。
    Control(ControlSignal),
    /// 不可恢复业务失败；保留动态源错误。
    Failure(BodyFailure),
}

impl BodyError {
    /// 构造不可恢复 Failure，保留动态源错误。
    pub fn fail<E: Error + 'static>(error: E) -> Self {
        Self::Failure(BodyFailure::new(error))
    }

    /// 构造 Retry 控制请求，保留动态原因。
    pub fn retry<E: Error + 'static>(error: E) -> Self {
        Self::Control(ControlSignal::Retry(RetryError::new(error)))
    }

    /// 构造 IterBreak 控制请求。
    pub fn iter_break() -> Self {
        Self::Control(ControlSignal::IterBreak(IterBreak))
    }
}

impl fmt::Display for BodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(signal) => write!(f, "control: {signal}"),
            Self::Failure(failure) => write!(f, "failure: {failure}"),
        }
    }
}

impl Error for BodyError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(signal) => Some(signal),
            Self::Failure(failure) => Some(failure),
        }
    }
}

/// 沿调用链传播的控制信号类型。
#[derive(Debug)]
pub enum ControlSignal {
    /// Retry 请求。
    Retry(RetryError),
    /// IterBreak 请求。
    IterBreak(IterBreak),
}

impl fmt::Display for ControlSignal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry(error) => write!(f, "{error}"),
            Self::IterBreak(_) => f.write_str("iter break"),
        }
    }
}

impl Error for ControlSignal {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Retry(error) => Some(error),
            Self::IterBreak(_) => None,
        }
    }
}

/// 不可恢复业务失败：动态 Error 对象及其 source chain。
///
/// 如果同一次 Root 退出时还发生了框架清理故障，首个清理诊断单独保存在
/// [`BodyFailure::cleanup_source`]：原业务对象仍是 `Error::source` 链的首项，
/// 两者互不覆盖。
#[derive(Debug)]
pub struct BodyFailure {
    error: Box<dyn Error + 'static>,
    cleanup: Option<Box<dyn Error + 'static>>,
}

impl BodyFailure {
    pub(crate) fn new<E: Error + 'static>(error: E) -> Self {
        Self {
            error: Box::new(error),
            cleanup: None,
        }
    }

    /// 附上首个清理失败诊断；不替换原业务对象。
    pub(crate) fn with_cleanup(mut self, cleanup: Option<&ScopeError>) -> Self {
        self.cleanup = cleanup.map(|error| {
            Box::new(FrameworkDiagnostic {
                scope_error: error.clone(),
            }) as Box<dyn Error + 'static>
        });
        self
    }

    /// 首个清理失败诊断（如有）；与 `Error::source` 的原业务错误分别保留。
    pub fn cleanup_source(&self) -> Option<&(dyn Error + 'static)> {
        self.cleanup.as_deref()
    }
}

impl fmt::Display for BodyFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "body failure: {}", self.error)?;
        if self.cleanup.is_some() {
            f.write_str(" (with an additional cleanup failure)")?;
        }
        Ok(())
    }
}

impl Error for BodyFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.error)
    }
}

/// Retry 控制请求的动态原因。
#[derive(Debug)]
pub struct RetryError {
    error: Box<dyn Error + 'static>,
}

impl RetryError {
    pub(crate) fn new<E: Error + 'static>(error: E) -> Self {
        Self {
            error: Box::new(error),
        }
    }
}

impl fmt::Display for RetryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "retry requested: {}", self.error)
    }
}

impl Error for RetryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&*self.error)
    }
}

/// IterBreak 控制请求；不携带业务状态。
#[derive(Clone, Copy, Debug)]
pub struct IterBreak;

impl fmt::Display for IterBreak {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("iter break")
    }
}

impl Error for IterBreak {}

// ---------------------------------------------------------------- Definition 构建错误

/// 使用位置与当前 Body 的关系；用于不可见来源的诊断。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Visibility {
    /// 位置属于使用 Body 的后代：child-local 直接泄漏。
    ChildLocal,
    /// 位置属于使用 Body 的兄弟子树：sibling 内部引用。
    Sibling,
}

/// Definition 构建失败明细（内部）：位置、Body／Step 上下文与原因。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BuildErrorDetail {
    /// 位置来自其他 Definition，不能混用。
    ForeignDefinition {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
        expected: DefinitionId,
        found: DefinitionId,
    },
    /// 位置未在该 Definition 登记，没有生产者或合法来源。
    UnknownPosition {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
    },
    /// 位置在该使用点之后才登记：前向／当时不可用的逻辑来源。
    NotYetAvailable {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
    },
    /// 位置不在使用 Body 的词法可见范围内。
    OutOfScope {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
        owner: BodyId,
        relation: Visibility,
    },
    /// 登记类型与声明类型不一致。
    TypeMismatch {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
        declared: TypeDesc,
        registered: TypeDesc,
    },
    /// Root 输出直接重复同一逻辑 Ref。
    DuplicateRootOutput { position: PositionId },
    /// Choose 配置：某个匹配值被登记了两次（按 `K` 的 `PartialEq` 识别）。
    DuplicateCase {
        body: BodyId,
        step: Option<usize>,
        first_slot: u16,
        second_slot: u16,
    },
    /// Choose 配置：登记了第二个 otherwise 分支。
    DuplicateOtherwise { body: BodyId, step: Option<usize> },
    /// Choose 配置：既没有 case 也没有 otherwise。
    EmptyChoose { body: BodyId, step: Option<usize> },
    /// Retry 预算：`max_retries = usize::MAX`，无法表达 N+1 次尝试（SPEC §10）。
    RetryBudgetUnrepresentable { body: BodyId, step: Option<usize> },
    /// Iter 预算：`max_iterations = 0`，至少需要一轮（SPEC §11）。
    IterZeroRounds { body: BodyId, step: Option<usize> },
    /// Core 位置分配（checked 序列）失败。
    PositionAllocationFailed,
    /// 当前使用点没有可见、已发布、未失效的同类型业务来源。
    #[allow(dead_code)] // 由内部 resolver 在测试/下游接入时构造
    MissingSource {
        body: BodyId,
        slot: Option<u16>,
        type_desc: TypeDesc,
    },
    /// 位置已经分配，但尚未发布到父域。
    NotPublished {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
    },
    /// 位置的生产登记已失效（依赖失败）。
    TaintedSource {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
    },
    /// 位置类别属于内部 Exit／Bridge／Local，不能作为业务来源。
    NotBusinessSource {
        body: BodyId,
        step: Option<usize>,
        position: PositionId,
        source: PositionSource,
    },
    /// Body 已经尝试声明；第二次声明不得覆盖或清除首次状态。
    DuplicateDeclaration {
        body: BodyId,
        first: DeclarationKind,
        second: DeclarationKind,
    },
    /// 该 Body 的用途不允许此声明种类。
    DeclarationNotAllowed {
        body: BodyId,
        kind: DeclarationKind,
        role: BodyRole,
    },
    /// 缺少完整声明（非 unit Root、Iter Round 的 Next）。
    MissingDeclaration { body: BodyId, kind: DeclarationKind },
    /// 声明 Shape 的槽数不符。
    DeclarationArityMismatch {
        body: BodyId,
        kind: DeclarationKind,
        slot: u16,
        expected: u16,
        found: u16,
    },
    /// 声明 Shape 的逐槽类型不符。
    DeclarationShapeMismatch {
        body: BodyId,
        kind: DeclarationKind,
        slot: u16,
        expected: TypeDesc,
        found: Option<TypeDesc>,
    },
    /// Iter Round 的 Next 状态不能是 unit。
    NextUnitUnsupported { body: BodyId },
    /// Choose 分支声明状态混合（未声明／显式 unit／非 unit）。
    ChooseDeclarationStateMismatch {
        body: BodyId,
        step: Option<usize>,
        first_branch: u16,
        second_branch: u16,
    },
    /// Choose 分支槽数不一致。
    ChooseArityMismatch {
        body: BodyId,
        step: Option<usize>,
        branch: u16,
        expected: u16,
        found: u16,
    },
    /// Choose 分支逐槽类型不一致。
    ChooseSlotTypeMismatch {
        body: BodyId,
        step: Option<usize>,
        branch: u16,
        slot: u16,
        expected: TypeDesc,
        found: TypeDesc,
    },
    /// 自动 Query 的输入 Shape 含重复类型槽位；需改用显式连接。
    ///
    /// 只适用于自动 Node 输入：显式连接与 output/next 声明不套用该规则。
    DuplicateQuery {
        body: BodyId,
        type_desc: TypeDesc,
        first_slot: u16,
        second_slot: u16,
    },
}

impl fmt::Display for BuildErrorDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignDefinition {
                body,
                step,
                position,
                expected,
                found,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} belongs to another definition \
                 (expected {expected:?}, found {found:?})"
            ),
            Self::UnknownPosition {
                body,
                step,
                position,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} has no registered source"
            ),
            Self::NotYetAvailable {
                body,
                step,
                position,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} is not available at this use"
            ),
            Self::OutOfScope {
                body,
                step,
                position,
                owner,
                relation,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} is not lexically visible \
                 (owner {owner:?}, relation {relation:?})"
            ),
            Self::TypeMismatch {
                body,
                step,
                position,
                declared,
                registered,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} is registered as \
                 {registered} but used as {declared}"
            ),
            Self::DuplicateRootOutput { position } => write!(
                f,
                "root output declares position {position:?} more than once"
            ),
            Self::DuplicateCase {
                body,
                step,
                first_slot,
                second_slot,
            } => write!(
                f,
                "choose at body {body:?} step {step:?} repeats a case value \
                 (branch {first_slot:?} and branch {second_slot:?})"
            ),
            Self::DuplicateOtherwise { body, step } => write!(
                f,
                "choose at body {body:?} step {step:?} registers a second otherwise branch"
            ),
            Self::EmptyChoose { body, step } => write!(
                f,
                "choose at body {body:?} step {step:?} has neither a case nor an otherwise"
            ),
            Self::RetryBudgetUnrepresentable { body, step } => write!(
                f,
                "retry at body {body:?} step {step:?} declares max_retries = usize::MAX, \
                 which cannot express the N+1 attempts budget"
            ),
            Self::IterZeroRounds { body, step } => write!(
                f,
                "iter at body {body:?} step {step:?} declares max_iterations = 0; at least \
                 one round is required"
            ),
            Self::PositionAllocationFailed => {
                f.write_str("core RefId allocation failed (identity space exhausted)")
            }
            Self::MissingSource {
                body,
                slot,
                type_desc,
            } => write!(
                f,
                "body {body:?} has no available business source of type {type_desc} at slot \
                 {slot:?}"
            ),
            Self::NotPublished {
                body,
                step,
                position,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} is allocated but not \
                 published to this use point"
            ),
            Self::TaintedSource {
                body,
                step,
                position,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} is tainted by a failed \
                 dependency"
            ),
            Self::NotBusinessSource {
                body,
                step,
                position,
                source,
            } => write!(
                f,
                "position {position:?} at body {body:?} step {step:?} is an internal \
                 {source:?} endpoint, not a business source"
            ),
            Self::DuplicateDeclaration {
                body,
                first,
                second,
            } => write!(
                f,
                "body {body:?} already attempted a {first:?} declaration; a second \
                 {second:?} declaration is rejected"
            ),
            Self::DeclarationNotAllowed { body, kind, role } => write!(
                f,
                "body {body:?} with role {role:?} cannot declare {kind:?}"
            ),
            Self::MissingDeclaration { body, kind } => {
                write!(
                    f,
                    "body {body:?} is missing its required {kind:?} declaration"
                )
            }
            Self::DeclarationArityMismatch {
                body,
                kind,
                slot,
                expected,
                found,
            } => write!(
                f,
                "body {body:?} {kind:?} declaration at slot {slot} has {found} slots, \
                 expected {expected}"
            ),
            Self::DeclarationShapeMismatch {
                body,
                kind,
                slot,
                expected,
                found,
            } => write!(
                f,
                "body {body:?} {kind:?} declaration slot {slot} has type {found:?}, \
                 expected {expected}"
            ),
            Self::NextUnitUnsupported { body } => {
                write!(f, "iter round body {body:?} cannot use a unit Next state")
            }
            Self::ChooseDeclarationStateMismatch {
                body,
                step,
                first_branch,
                second_branch,
            } => write!(
                f,
                "choose at body {body:?} step {step:?} mixes declaration states between \
                 branches {first_branch} and {second_branch}"
            ),
            Self::ChooseArityMismatch {
                body,
                step,
                branch,
                expected,
                found,
            } => write!(
                f,
                "choose at body {body:?} step {step:?} branch {branch} has {found} output \
                 slots, expected {expected}"
            ),
            Self::ChooseSlotTypeMismatch {
                body,
                step,
                branch,
                slot,
                expected,
                found,
            } => write!(
                f,
                "choose at body {body:?} step {step:?} branch {branch} slot {slot} has type \
                 {found}, expected {expected}"
            ),
            Self::DuplicateQuery {
                body,
                type_desc,
                first_slot,
                second_slot,
            } => write!(
                f,
                "body {body:?} automatic Query declares type {type_desc} at more than one input \
                 slot (slot {first_slot} and slot {second_slot}); connect the same-type inputs \
                 explicitly with then_with/then_with_output"
            ),
        }
    }
}

/// Definition 构建失败（公开）：只提供稳定文本与 `Error` 接口，不暴露内部位置身份。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BuildError {
    pub(crate) detail: BuildErrorDetail,
}

impl BuildError {
    pub(crate) fn new(detail: BuildErrorDetail) -> Self {
        Self { detail }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.detail)
    }
}

impl Error for BuildError {}

// ---------------------------------------------------------------- 运行期诊断

/// 框架运行期故障的诊断包装；`Display` 输出稳定文本，`source` 保留原始 Scope 诊断。
#[derive(Debug)]
pub(crate) struct FrameworkDiagnostic {
    scope_error: ScopeError,
}

impl fmt::Display for FrameworkDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.scope_error)
    }
}

impl Error for FrameworkDiagnostic {}

/// Root 执行期间的框架诊断（公开）：稳定文本 + 可选原始诊断链。
#[derive(Debug)]
pub struct RuntimeError {
    note: &'static str,
    source: Option<Box<dyn Error + 'static>>,
    cleanup: Option<Box<dyn Error + 'static>>,
}

impl RuntimeError {
    /// 以任意动态诊断作为 `source` 构造（例如被清理故障取代的控制信号）。
    pub(crate) fn with_source(note: &'static str, source: Box<dyn Error + 'static>) -> Self {
        Self {
            note,
            source: Some(source),
            cleanup: None,
        }
    }

    pub(crate) fn new(note: &'static str, scope_error: Option<&ScopeError>) -> Self {
        Self {
            note,
            source: scope_error.map(|error| {
                Box::new(FrameworkDiagnostic {
                    scope_error: error.clone(),
                }) as Box<dyn Error + 'static>
            }),
            cleanup: None,
        }
    }

    /// 附上首个清理失败诊断；首错与首清理诊断互不覆盖。
    pub(crate) fn with_cleanup(mut self, cleanup: Option<&ScopeError>) -> Self {
        self.cleanup = cleanup.map(|error| {
            Box::new(FrameworkDiagnostic {
                scope_error: error.clone(),
            }) as Box<dyn Error + 'static>
        });
        self
    }

    /// 稳定说明文本。
    pub fn note(&self) -> &'static str {
        self.note
    }

    /// 首个清理失败诊断（如有）；与 `Error::source` 的首个执行诊断分别保留。
    pub fn cleanup_source(&self) -> Option<&(dyn Error + 'static)> {
        self.cleanup.as_deref()
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.note)?;
        if self.cleanup.is_some() {
            f.write_str(" (with an additional cleanup failure)")?;
        }
        Ok(())
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref()
    }
}

/// Root 执行结果错误（SPEC §12 分类）。
#[derive(Debug)]
pub enum RunError {
    /// Definition 构建／验证失败；任何业务 Node 未开始执行。
    Definition(BuildError),
    /// 业务 Failure：原动态错误对象与 source chain 保留。
    Body(BodyFailure),
    /// Retry 预算耗尽（控制器随 T04 接入；当前主链不产生）。
    RetryExhausted {
        /// 配置的最大重试次数。
        max_retries: usize,
        /// 实际尝试次数。
        attempts: usize,
        /// 最后一次 Retry 的动态原因。
        last_error: RetryError,
    },
    /// Iter 轮次上限（控制器随 T04 接入；当前主链不产生）。
    IterationLimitReached {
        /// 配置的最大轮次。
        max_iterations: usize,
        /// 已完成轮次。
        completed_iterations: usize,
    },
    /// 到达 Root 仍未被处理的控制信号。
    UnhandledControl(ControlSignal),
    /// Choose 无匹配 case 且没有 otherwise。
    ///
    /// 若同一次失败退出同时发生清理故障，公开结果是 [`RunError::Runtime`]：其
    /// `Error::source` 保留本分类作为原终止原因，`cleanup_source` 保留首个清理故障诊断。
    NoMatchingCase,
    /// 框架运行期故障。
    Runtime(RuntimeError),
}

impl fmt::Display for RunError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Definition(error) => write!(f, "definition rejected: {error}"),
            Self::Body(failure) => write!(f, "{failure}"),
            Self::RetryExhausted {
                max_retries,
                attempts,
                last_error,
            } => write!(
                f,
                "retry exhausted after {attempts} attempts (max_retries {max_retries}): {last_error}"
            ),
            Self::IterationLimitReached {
                max_iterations,
                completed_iterations,
            } => write!(
                f,
                "iteration limit {max_iterations} reached after {completed_iterations} rounds"
            ),
            Self::UnhandledControl(signal) => write!(f, "unhandled control: {signal}"),
            Self::NoMatchingCase => f.write_str("no matching case and no otherwise branch"),
            Self::Runtime(error) => write!(f, "runtime error: {error}"),
        }
    }
}

impl Error for RunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Definition(error) => Some(error),
            Self::Body(failure) => Some(failure),
            Self::RetryExhausted { last_error, .. } => Some(last_error),
            Self::IterationLimitReached { .. } => None,
            Self::UnhandledControl(signal) => Some(signal),
            Self::NoMatchingCase => None,
            Self::Runtime(error) => Some(error),
        }
    }
}

// ---------------------------------------------------------------- 执行链内部失败

/// 一次 Body／Step 执行的内部失败：控制信号、业务失败、无匹配 case 或框架故障。
///
/// `NoMatchingCase` 是不可恢复终止而不是控制信号：它不寻找边界、不被任何分支接管；
/// 只有 Root 收口把它映射为公开 [`RunError::NoMatchingCase`]（叠加清理故障时按
/// [`RunError::Runtime`] 保留原终止原因，见 Engine §6.7 与 WE-T03 §6.1）。
#[derive(Debug)]
pub(crate) enum ExecFailure {
    /// 未处理的控制信号；沿调用链上传。
    Control(ControlSignal),
    /// 业务不可恢复失败。
    Failure(BodyFailure),
    /// Choose 无匹配 case 且没有 otherwise。
    NoMatchingCase,
    /// 控制器预算终止错误；经 Root 转成冻结 RunError，不是控制信号。
    Budget(BudgetTermination),
    /// 框架拒绝／清理故障。
    Runtime(RuntimeError),
}

/// 控制器预算终止错误（SPEC §10／§11、Engine §6.8／§6.9）。
///
/// 它不是控制信号：不会被任何边界当作信号捕获，沿调用链直接终止，并由 Root 转成对应
/// 的冻结 [`RunError`] 变体；叠加清理故障时的公开分类见 SPEC §12。
#[derive(Debug)]
pub(crate) enum BudgetTermination {
    /// Retry 预算耗尽：保留配置、实际尝试次数与最后一次动态原因。
    RetryExhausted {
        max_retries: usize,
        attempts: usize,
        last_error: RetryError,
    },
    /// Iter 轮次上限：保留配置与已完成轮次。
    IterationLimitReached {
        max_iterations: usize,
        completed_iterations: usize,
    },
}

impl BudgetTermination {
    /// 转成冻结的公开分类（不含清理诊断）。
    pub(crate) fn into_run_error(self) -> RunError {
        match self {
            Self::RetryExhausted {
                max_retries,
                attempts,
                last_error,
            } => RunError::RetryExhausted {
                max_retries,
                attempts,
                last_error,
            },
            Self::IterationLimitReached {
                max_iterations,
                completed_iterations,
            } => RunError::IterationLimitReached {
                max_iterations,
                completed_iterations,
            },
        }
    }
}

impl ExecFailure {
    /// 由 Scope 诊断构造框架故障。
    pub(crate) fn runtime(note: &'static str, error: &ScopeError) -> Self {
        Self::Runtime(RuntimeError::new(note, Some(error)))
    }

    /// 由可选 Scope 诊断构造框架故障。
    pub(crate) fn runtime_opt(note: &'static str, error: Option<&ScopeError>) -> Self {
        Self::Runtime(RuntimeError::new(note, error))
    }
}

impl fmt::Display for ExecFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(signal) => write!(f, "{signal}"),
            Self::Failure(failure) => write!(f, "{failure}"),
            Self::NoMatchingCase => f.write_str("choose has no matching case"),
            Self::Budget(budget) => match budget {
                BudgetTermination::RetryExhausted {
                    max_retries,
                    attempts,
                    last_error,
                } => write!(
                    f,
                    "retry exhausted after {attempts} attempts (max_retries {max_retries}): {last_error}"
                ),
                BudgetTermination::IterationLimitReached {
                    max_iterations,
                    completed_iterations,
                } => write!(
                    f,
                    "iteration limit {max_iterations} reached after {completed_iterations} rounds"
                ),
            },
            Self::Runtime(error) => write!(f, "{error}"),
        }
    }
}
