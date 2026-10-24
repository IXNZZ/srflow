//! SRFlow Core Runtime 的内部模块。
//!
//! 这里的内容不对外导出，业务侧无法访问 DataContainer、ID 或存储操作。V21-01 交付
//! Execution 身份、异构 owned Data 存储与短期只读借用；V21-02 交付 Scope registry、
//! Import／Export 与同步退出边界；V21-03 交付 `CollectorId` 身份、控制状态登记与状态
//! 来源导入、Promote、受控 pending 回收、collector 建构区与 ItemScope 直接 Consume；
//! V21-04 交付一次 Execution 的内部调用设施（单 Context、Invocation frame 与退出
//! guard、共享重借用下的异步借用边界及执行错误／取消清理）；V21-05～V21-11 交付旧
//! DataRef／Signature／业务 Node 与 Orchestrator 协议、Flow／Match／Each／Loop 上层
//! 与旧 Application `Runtime`；V21-12 起旧能力曾经 `crate::api` facade 导出。
//!
//! **WE-T06 B 阶段清理由此范围终止**：旧公开门面（`crate::api` 与 `lib.rs::legacy`）
//! 与旧执行／构建上层协议（builder／flow／data_ref／node／match_orchestrator／each／
//! loop_orchestrator／root_signature）已退出（X29）；下述模块是清理后仍保留的数据与
//! 执行基础（X28／X30 组件）。WE-T01 起新增 `engine` 子模块：新 Public 契约的声明
//! 协议、Definition 构建／验证与真实执行主链；它保留 crate 内部可达性，不替代下方
//! 保留的 Core 数据／调用设施。
//!
//! 保留模块职责（B 阶段后）：
//! - [`context`]：单 Context、Invocation frame 与退出 guard、终结／清理诊断与
//!   Root 提取预检／take；Core 内部 [`context::BodyError`] 与取消清理；
//! - [`scope`]：ScopeCoordinator、Import／Export／Alias、cap／CollectionRoot、
//!   成组 Consume／Promote 与窄许可；
//! - [`data_container`]：异构 owned Data 存储与只读借用；
//! - [`identity`]／[`ref_id`]：Execution 身份与 `DataId`／`ScopeId`／`RefId` 分配；
//! - [`signature`]：仅保留共用 [`signature::DeclaredPort`]（声明端口元数据）；
//! - [`runtime`]：仅保留生产向 [`runtime::RootExecution`] 与 `cfg(test)` Root 驱动；
//! - `orchestrator`：仅保留 `cfg(test)` 只读观察标签 `ScopeRole`；
//! - [`internal_error`]：内部诊断载体。

pub(crate) mod context;
pub(crate) mod data_container;
pub(crate) mod engine;
pub(crate) mod identity;
pub(crate) mod internal_error;
pub(crate) mod orchestrator;
pub(crate) mod ref_id;
pub(crate) mod runtime;
pub(crate) mod scope;
pub(crate) mod signature;

#[cfg(test)]
#[path = "../../tests/internal/core/test_support.rs"]
mod test_support;
#[cfg(test)]
#[path = "../../tests/internal/core/v21_08_tests_cap.rs"]
mod v21_08_tests_cap;
#[cfg(test)]
#[path = "../../tests/internal/core/v21_10_tests_defence.rs"]
mod v21_10_tests_defence;
