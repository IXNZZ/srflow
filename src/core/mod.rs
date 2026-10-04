//! SRFlow Core Runtime 的内部模块。
//!
//! 这里的内容不对外导出，业务侧无法访问 DataContainer、ID 或存储操作。
//! V21-01 交付 Execution 身份、异构 owned Data 存储与短期只读借用；V21-02 交付
//! Scope registry、Import／Export 与同步退出边界；V21-03 交付 `CollectorId` 身份、
//! 控制状态登记与状态来源导入、Promote、受控 pending 回收、collector 建构区与
//! ItemScope 直接 Consume；V21-04 交付一次 Execution 的内部调用设施：单 Context、
//! Invocation frame 与退出 guard、共享重借用下的异步借用边界及执行错误／取消清理
//! （V21-04 已验收；G21-A 另行审查）。

pub(crate) mod context;
pub(crate) mod data_container;
pub(crate) mod identity;
pub(crate) mod internal_error;
pub(crate) mod ref_id;
pub(crate) mod runtime;
pub(crate) mod scope;
