//! 一次 Root Execution 的拥有者与测试驱动。
//!
//! `RootExecution` 是内部 Root 设施：它创建身份根与 [`ExecutionContext`]，并作为本次
//! Execution 的唯一拥有者。nested 调用只接收 `&mut ExecutionContext`，没有重建 Root
//! 或替换 Container 的入口，因此一次 Execution 始终只有一个身份根、一个协调组件与
//! 唯一 DataContainer。
//!
//! B 阶段清理后本模块只保留两项共用设施：生产向的 [`RootExecution`]（新 Engine 的
//! `Runtime::execute` 与 `engine/runtime.rs` 的真实 Root 入口使用），以及 `cfg(test)`
//! 的 [`RootExit`]／[`run_root`] 受控 Root 驱动（Core `context.rs` 的清理／终止／
//! 取消不变量样本使用）。旧 V21-10 的应用层 `Runtime::execute`／`RootError`／装配
//! 预检与 fault 注入已随旧上层协议退出。

use super::context::ExecutionContext;
#[cfg(test)]
use super::context::{
    BodyError, CleanupDiagnostic, ExecutionTermination, InvocationKind, TerminationKind,
};
use super::identity::ExecutionIdentity;
#[cfg(test)]
use super::identity::ScopeId;
#[cfg(test)]
use super::internal_error::ScopeError;

/// 本次 Root Execution 的拥有者。
///
/// 丢弃它（例如丢弃持有它的 Root Future）即销毁 Context 与其中唯一 DataContainer；
/// 丢弃前若仍有未完成调用，由对应 guard 的取消清理负责。
pub(crate) struct RootExecution {
    context: ExecutionContext,
}

impl RootExecution {
    /// 创建一次 Root Execution：新身份根 + 新 Context（含唯一协调组件与 Container）。
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

/// 受控 Root 驱动：拥有 Context 的 async 执行体。
///
/// 进入 Root frame 后运行 `body`；成功返回且本次执行未被终止时，沿已验收的 finalization
/// 路径以**空声明输出与空输出位置**关闭 RootScope。执行体失败、收口失败或执行已被取消时
/// 都走受控失败退出：由 guard 清理 RootScope 与未完成责任，并保留首次原因与清理诊断。
///
/// `config` 是 Root 执行体自带的非 Data 上下文参数，驱动原样透传，不解释也不存储；
/// `config` 可以是借用。驱动只做 Root frame 建立与收口，不解释业务输入／输出。
#[cfg(test)]
#[path = "../../tests/internal/core/runtime_observe.rs"]
pub(crate) mod observe;
