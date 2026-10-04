//! 一次 Root Execution 的拥有者与驱动。
//!
//! `RootExecution` 是内部 Root 设施：它创建身份根与 [`ExecutionContext`]，并作为本次
//! Execution 的唯一拥有者。nested 调用只接收 `&mut ExecutionContext`，没有重建 Root
//! 或替换 Container 的入口，因此一次 Execution 始终只有一个身份根、一个协调组件与
//! 唯一 DataContainer。
//!
//! 完整 `Runtime::execute(root, input) -> owned Output` 属 V21-10；本模块只交付 Root 的
//! 执行设施与资源生命周期：成功收口固定使用**空声明输出与空输出位置**的 finalization
//! 路径关闭 RootScope，不实现 take，也不据此支持零输入 Flow。

use super::context::{
    BodyError, CleanupDiagnostic, ExecutionContext, ExecutionTermination, InvocationKind,
    TerminationKind,
};
use super::identity::{ExecutionIdentity, ScopeId};
use super::internal_error::ScopeError;

/// 本次 Root Execution 的拥有者。
///
/// 丢弃它（例如丢弃持有它的 Root Future）即销毁 Context 与其中唯一 DataContainer；
/// 丢弃前若仍有未完成调用，由对应 guard 的取消清理负责。
#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
pub(crate) struct RootExecution {
    context: ExecutionContext,
}

#[allow(dead_code)] // 非 test 构建下无生产消费者；由 V21-04／V21-05 的验收样本与后续任务驱动
impl RootExecution {
    /// 创建一次 Root Execution：新身份根 + 新 Context（含唯一协调组件与 Container）。
    #[allow(dead_code)] // V21-10 接入真实 Runtime::execute 与公开结果记录前只由内部驱动与测试使用
    pub(crate) fn start() -> Self {
        Self {
            context: ExecutionContext::new(ExecutionIdentity::new()),
        }
    }

    /// 当前 Context 的只读视图。
    pub(crate) fn context(&self) -> &ExecutionContext {
        &self.context
    }

    /// 当前 Context 的可变视图（供 Root 驱动使用）。
    pub(crate) fn context_mut(&mut self) -> &mut ExecutionContext {
        &mut self.context
    }
}
/// Root 驱动的收口结果：拥有执行体错误、首次终止原因、Root 关闭诊断与首个清理诊断。
///
/// 这是内部驱动的结果记录，不是公开 Execution Error API。原执行失败与清理失败都在这里
/// 保留，互不覆盖，也不会在收口时被丢弃。
#[derive(Debug)]
#[allow(dead_code)] // V21-10 接入真实 Runtime::execute 与公开结果记录前只由内部驱动与测试使用
pub(crate) struct RootExit {
    #[allow(dead_code)]
    body_error: Option<BodyError>,
    termination: Option<ExecutionTermination>,
    close_error: Option<ScopeError>,
    cleanup_failure: Option<CleanupDiagnostic>,
    #[cfg(test)]
    cleanup_events: usize,
}

#[allow(dead_code)] // V21-10 接入真实 Runtime::execute 与公开结果记录前只由内部驱动与测试使用
impl RootExit {
    /// 执行体的原始失败（含说明与可选 Scope 诊断）。
    #[allow(dead_code)] // V21-10 接入真实 Runtime::execute 与公开结果记录前只由内部驱动与测试使用
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
    #[cfg(test)]
    pub(crate) fn cleanup_events(&self) -> usize {
        self.cleanup_events
    }
}

/// Root 驱动：拥有 Context 的 async 执行体。
///
/// 进入 Root frame 后运行 `body`；成功返回且本次执行未被终止时，沿已验收的 finalization
/// 路径以**空声明输出与空输出位置**关闭 RootScope。执行体失败、收口失败或执行已被取消时
/// 都走受控失败退出：由 guard 清理 RootScope 与未完成责任，并保留首次原因与清理诊断。
///
/// `config` 是 Root 执行体自带的非 Data 上下文参数（V21-10 的 Root Orchestrator 与
/// 定义），驱动原样透传，不解释也不存储。V21-05 起 `config` 可以是借用（例如
/// `&Definition`）：驱动不要求 `'static`，也不把借用存进任何长期结构。
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
