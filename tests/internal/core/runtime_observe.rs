//! 本模块为原生产文件中测试观测/故障注入实现（TEST-CLEANUP-01 迁出）；
//! 由原文件以 `#[cfg(test)] #[path = ...] pub(crate) mod observe;` 加载，运行期不参与编译。
use crate::core::runtime::*;

#[cfg(test)]
impl RootExit {
    /// 执行体的原始失败（含说明与可选 Scope 诊断）。
    pub(crate) fn body_error(&self) -> Option<&BodyError> {
        self.body_error.as_ref()
    }
    /// 首次终止类别（如有）。
    pub(crate) fn terminated(&self) -> Option<TerminationKind> {
        self.termination
            .as_ref()
            .map(|termination| termination.kind())
    }
    /// 首次终止原因说明（如有），例如执行体失败的原文。
    pub(crate) fn termination_note(&self) -> Option<&'static str> {
        self.termination
            .as_ref()
            .map(|termination| termination.note())
    }
    /// 首次终止相关的 Scope（如有）。
    pub(crate) fn termination_scope(&self) -> Option<&ScopeId> {
        self.termination
            .as_ref()
            .and_then(|termination| termination.scope())
    }
    /// RootScope 正常收口的诊断（如有）。
    pub(crate) fn close_error(&self) -> Option<&ScopeError> {
        self.close_error.as_ref()
    }
    /// 首次失败／取消保存的原始 Scope 诊断（如有）。
    pub(crate) fn termination_scope_error(&self) -> Option<&ScopeError> {
        self.termination
            .as_ref()
            .and_then(|termination| termination.scope_error())
    }
    /// 首个清理失败的故障 Scope（如有）。
    pub(crate) fn cleanup_scope(&self) -> Option<&ScopeId> {
        self.cleanup_failure
            .as_ref()
            .map(|diagnostic| diagnostic.scope())
    }
    /// 首个清理失败的诊断（如有）。
    pub(crate) fn cleanup_error(&self) -> Option<&ScopeError> {
        self.cleanup_failure
            .as_ref()
            .map(|diagnostic| diagnostic.error())
    }
    /// guard 在退出中执行清理的次数（仅测试观测）。
    pub(crate) fn cleanup_events(&self) -> usize {
        self.cleanup_events
    }
}

pub(crate) async fn run_root<X, F>(mut execution: RootExecution, config: X, body: F) -> RootExit
where
    F: for<'a> AsyncFnOnce(&'a mut ExecutionContext, X) -> Result<(), BodyError>,
{
    let root_scope = execution.context.root_scope();
    let mut close_error: Option<ScopeError> = None;

    let body_error = {
        let mut guard = execution
            .context
            .enter(InvocationKind::Root, &root_scope, true)
            .expect("a fresh execution context accepts its root frame");

        let body_result = body(&mut guard, config).await;
        match &body_result {
            Ok(()) => {
                if guard.is_terminated() {
                    // 执行体返回 Ok，但本次执行已被取消或失败：不能正常收口，
                    // 必须走受控失败退出，由 guard 清理 RootScope 与未完成责任。
                    guard.failed("root closed by a terminated execution");
                } else {
                    // 成功收口：Root frame 仍在，RootScope 属于当前调用的可见范围。
                    match guard.finalize(&root_scope, &[], &mut Vec::new()) {
                        Ok(()) => guard.complete(),
                        Err(error) => {
                            close_error = Some(error);
                            guard.failed("root finalization failed");
                        }
                    }
                }
            }
            Err(error) => guard.failed(error.note()),
        }
        body_result.err()
    };

    // 只取诊断副本：Context 自身的终止状态保持不可恢复。
    let termination = execution.context.termination_report();
    let cleanup_failure = execution.context.cleanup_report();
    RootExit {
        body_error,
        termination,
        close_error,
        cleanup_failure,
        #[cfg(test)]
        cleanup_events: execution.context.cleanup_events(),
    }
}

/// Root 驱动的收口结果：拥有执行体错误、首次终止原因、Root 关闭诊断与首个清理诊断。
///
/// 这是内部驱动的结果记录，不是公开 Execution Error API。原执行失败与清理失败都在这里
/// 保留，互不覆盖，也不会在收口时被丢弃。
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct RootExit {
    body_error: Option<BodyError>,
    termination: Option<ExecutionTermination>,
    close_error: Option<ScopeError>,
    cleanup_failure: Option<CleanupDiagnostic>,
    #[cfg(test)]
    cleanup_events: usize,
}
