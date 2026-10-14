//! 公开 Root 入口：`Runtime::execute`（SPEC §4、Engine §2.7、§4）。
//!
//! Future 第一次被驱动时同步构建并验证唯一 Definition；随后建立唯一
//! RootExecution／ExecutionContext／DataContainer，登记 owned Root 输入，在真实 Root
//! frame 中顺序执行 Node／Chain／Choose／Retry／Iter，最后整组提取 owned 输出。失败返回 [`RunError`]，
//! 不返回部分输出。

use crate::core::context::{CleanupDiagnostic, InvocationGuard, InvocationKind};
use crate::core::engine::boundary::run_body;
use crate::core::engine::definition::{Definition, build_definition};
use crate::core::engine::error::{BuildError, ExecFailure, RunError, RuntimeError};
use crate::core::engine::flow::Flow;
use crate::core::engine::identity::BodyId;
use crate::core::engine::shape::{InputOps, RefShape, RootInput, RootInputOps, ShapeOps};
use crate::core::identity::ScopeId;
use crate::core::runtime::RootExecution;
use std::any::Any;
#[cfg(test)]
use std::cell::Cell;

#[cfg(test)]
thread_local! {
    /// cfg(test)：登记 owned 输入后销毁前 N 个 entry，使 Root 退出清理真实失败。
    static ROOT_CLEANUP_FAULT: Cell<usize> = const { Cell::new(0) };
}

/// cfg(test)：安装一次 Root 退出清理故障（业务 Failure／Control 收口的清理诊断样本）。
#[cfg(test)]
pub(crate) fn install_root_cleanup_fault() {
    install_root_cleanup_faults(1);
}

/// cfg(test)：安装 N 次 Root 退出清理故障（后续故障不得覆盖首清理诊断的样本）。
#[cfg(test)]
pub(crate) fn install_root_cleanup_faults(count: usize) {
    ROOT_CLEANUP_FAULT.with(|fault| fault.set(count));
}

#[cfg(test)]
fn take_root_cleanup_fault() -> usize {
    ROOT_CLEANUP_FAULT.with(|fault| fault.replace(0))
}

/// Application 侧唯一的 Root 执行入口。
///
/// 无状态：不缓存上一次的 Context／输入／结果／终止状态。每次调用创建新的
/// ExecutionIdentity、Context（含唯一 Container）与 RootScope；同一个定义闭包可以执行
/// 多次，每次独立身份空间。未被 poll 时尚未构建 Definition，输入随 Future 一同销毁。
///
/// 当前可执行结构为 Node、Chain（`Flow::chain`）、Each（`Flow::each`）、Choose
/// （`Flow::choose`）、Retry（`Flow::retry`）与 Iter（`Flow::iter`）。
#[derive(Debug, Default, Clone, Copy)]
pub struct Runtime;

impl Runtime {
    /// 创建执行入口。
    pub fn new() -> Self {
        Self
    }

    /// 执行一次 Definition：构建 → 验证 → 运行 Node／Chain／Choose／Retry／Iter → 整组提取 owned 输出。
    ///
    /// 顺序为“Definition 构建与验证 → Root 输入登记 → Root frame 内顺序执行 → 冻结／
    /// 整组预检 → 同步 take 并解除 Root 责任 → 关闭 Root”。任何一步失败都不返回部分
    /// owned 输出；构建失败不建立 RootExecution，也不执行任何业务 Node。
    #[allow(private_bounds)]
    pub async fn execute<'n, In, F, Out>(
        &self,
        definition: F,
        input: In,
    ) -> Result<Out::Owned, RunError>
    where
        In: RootInput + RootInputOps + InputOps,
        In::Refs: ShapeOps + RefShape,
        Out: RefShape + ShapeOps,
        F: FnOnce(&mut Flow<'n>, In::Refs) -> Out,
    {
        // 1) Definition：本次 Future 第一次被驱动时同步构建并验证。
        let built = build_definition::<In, F, Out>(definition)
            .map_err(|detail| RunError::Definition(BuildError::new(detail)))?;

        // 2) 新 Execution 与 owned Root 输入登记：只走正式 register_owned_erased；
        //    失败不执行业务体，已登记的值随本次 Execution 清理。
        let mut execution = RootExecution::start();
        let root = execution.context().root_scope();
        let mut pending = input.into_values().into_iter();
        #[cfg(test)]
        let mut input_ids: Vec<crate::core::identity::DataId> = Vec::new();
        for record in built.root_inputs() {
            let Some((type_name, value)) = pending.next() else {
                break;
            };
            match execution.context_mut().register_owned_erased(
                &root,
                record.core(),
                type_name,
                value,
            ) {
                Ok(id) => {
                    #[cfg(test)]
                    input_ids.push(id);
                    #[cfg(not(test))]
                    let _ = id;
                }
                Err(error) => {
                    return Err(RunError::Runtime(RuntimeError::new(
                        "root input registration failed",
                        Some(&error),
                    )));
                }
            }
        }
        drop(pending);
        #[cfg(test)]
        {
            let faults = take_root_cleanup_fault();
            for id in input_ids.iter().take(faults) {
                execution.context_mut().destroy_probe(id);
            }
        }

        // 3) Root frame：执行 → 整组提取 → 关闭；失败保留首次执行失败。
        let mut failure: Option<ExecFailure> = None;
        let mut values: Vec<Box<dyn Any>> = Vec::new();
        {
            let mut guard = execution
                .context_mut()
                .enter(InvocationKind::Root, &root, true)
                .expect("a fresh execution context accepts its root frame");
            #[cfg(test)]
            crate::core::engine::observe::record_frame(crate::core::engine::observe::FrameRecord {
                kind: "root",
                phase: "enter",
                scope: root.clone(),
            });
            let body = run_body(&mut guard, &built, BodyId::ROOT, &root).await;
            match body {
                Ok(()) if !guard.is_terminated() => match extract_root(&mut guard, &root, &built) {
                    Ok(owned) => {
                        values = owned;
                        guard.complete();
                    }
                    Err(error) => {
                        guard.failed("root output extraction rejected");
                        failure = Some(error);
                    }
                },
                Ok(()) => {
                    guard.failed("root closed by a terminated execution");
                    failure = Some(ExecFailure::runtime_opt(
                        "root closed by a terminated execution",
                        None,
                    ));
                }
                Err(error) => {
                    guard.failed("root body failed");
                    failure = Some(error);
                }
            }
        }
        #[cfg(test)]
        crate::core::engine::observe::record_frame(crate::core::engine::observe::FrameRecord {
            kind: "root",
            phase: "exit",
            scope: root.clone(),
        });

        // 4) 收口：首错与首清理诊断分别保留，互不覆盖。
        match failure {
            None => Ok(<Out as ShapeOps>::from_owned(&mut values.into_iter())),
            Some(failure) => Err(into_run_error(
                failure,
                execution.context().cleanup_report(),
            )),
        }
    }
}

/// Root owned 提取：冻结 → 整组预检 → 同步提交 → 关闭；预检拒绝时 take 计数为零。
fn extract_root(
    guard: &mut InvocationGuard<'_>,
    root: &ScopeId,
    definition: &Definition<'_>,
) -> Result<Vec<Box<dyn Any>>, ExecFailure> {
    let ports = definition.root_output_ports();
    let plan = guard
        .prepare_root_extraction(root, &ports)
        .map_err(|error| {
            #[cfg(test)]
            crate::core::test_support::record_root_snapshot(
                crate::core::test_support::root_snapshot(
                    guard,
                    root,
                    crate::core::test_support::RootSnapshotPhase::PreflightRejected,
                    0,
                    &[],
                ),
            );
            ExecFailure::runtime("root extraction preflight rejected", &error)
        })?;
    #[cfg(test)]
    let taken_ids = plan.taken_ids();
    #[cfg(test)]
    let expected = plan.take_count();
    let (owned, close_targets) = guard.commit_root_extraction(root, plan);
    #[cfg(test)]
    crate::core::test_support::record_root_snapshot(crate::core::test_support::root_snapshot(
        guard,
        root,
        crate::core::test_support::RootSnapshotPhase::AfterCommit,
        expected,
        &taken_ids,
    ));
    match guard.close_root(root, close_targets) {
        Ok(()) => {
            #[cfg(test)]
            crate::core::test_support::record_root_snapshot(
                crate::core::test_support::root_snapshot(
                    guard,
                    root,
                    crate::core::test_support::RootSnapshotPhase::AfterClose,
                    expected,
                    &taken_ids,
                ),
            );
            Ok(owned)
        }
        Err(error) => {
            // 已移交的值不再属于 Container：不返回给 Application，也不重复销毁。
            drop(owned);
            Err(ExecFailure::runtime(
                "root close after extraction failed",
                &error,
            ))
        }
    }
}

/// 映射为公开错误：所有类别都保留首个清理诊断，原业务对象／控制信号不丢失。
///
/// - 业务 Failure：分类保持 `Body`，原动态对象仍在 `Error::source` 链，清理诊断经
///   [`crate::BodyFailure::cleanup_source`] 取得；
/// - 未处理 Control 且 Root 清理失败：按 Engine §8.8 不再返回可恢复控制，分类转为
///   `Runtime`，`source` 保留原控制信号、`cleanup_source` 保留首个清理失败；
/// - 框架故障：`source` 为首个执行诊断，`cleanup_source` 为首个清理失败。
fn into_run_error(failure: ExecFailure, cleanup: Option<CleanupDiagnostic>) -> RunError {
    let cleanup = cleanup.as_ref().map(|diagnostic| diagnostic.error());
    match failure {
        ExecFailure::Runtime(error) => RunError::Runtime(error.with_cleanup(cleanup)),
        ExecFailure::Failure(failure) => RunError::Body(failure.with_cleanup(cleanup)),
        // 无匹配且无 otherwise：没有额外清理故障时保持冻结分类；叠加清理故障时按
        // WE-T03 §6.1 的已审定安排返回 Runtime，并以原有 RunError 保留原终止原因。
        ExecFailure::NoMatchingCase => match cleanup {
            Some(cleanup_error) => RunError::Runtime(
                RuntimeError::with_source(
                    "choose had no matching case and ended with a cleanup failure",
                    Box::new(RunError::NoMatchingCase),
                )
                .with_cleanup(Some(cleanup_error)),
            ),
            None => RunError::NoMatchingCase,
        },
        // 预算终止错误（RetryExhausted／IterationLimitReached）：没有额外清理故障时保持
        // 冻结分类与全部字段；叠加清理故障时按 SPEC §12 的已确认规则返回 Runtime，并以原有
        // 分类对象保留原始预算错误（其 source 继续指向最后动态原因）。
        ExecFailure::Budget(budget) => {
            let error = budget.into_run_error();
            match cleanup {
                Some(cleanup_error) => RunError::Runtime(
                    RuntimeError::with_source(
                        "budget exhaustion ended with a cleanup failure",
                        Box::new(error),
                    )
                    .with_cleanup(Some(cleanup_error)),
                ),
                None => error,
            }
        }
        ExecFailure::Control(signal) => match cleanup {
            Some(cleanup_error) => RunError::Runtime(
                RuntimeError::with_source(
                    "unhandled control ended with a cleanup failure",
                    Box::new(signal),
                )
                .with_cleanup(Some(cleanup_error)),
            ),
            None => RunError::UnhandledControl(signal),
        },
    }
}
