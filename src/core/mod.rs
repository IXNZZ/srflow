//! SRFlow Core Runtime 的内部模块。
//!
//! 这里的内容不对外导出，业务侧无法访问 DataContainer、ID 或存储操作。
//! V21-01 交付 Execution 身份、异构 owned Data 存储与短期只读借用；V21-02 交付
//! Scope registry、Import／Export 与同步退出边界；V21-03 交付 `CollectorId` 身份、
//! 控制状态登记与状态来源导入、Promote、受控 pending 回收、collector 建构区与
//! ItemScope 直接 Consume；V21-04 交付一次 Execution 的内部调用设施：单 Context、
//! Invocation frame 与退出 guard、共享重借用下的异步借用边界及执行错误／取消清理
//! （V21-04 已验收；G21-A 另行审查）。V21-05 交付 DataRef／Signature、业务 Node 与
//! Orchestrator 协议、强类型异构 CallSite 及真实 Context 双路径分派；V21-06 交付完整
//! 内部 Flow 与 SubFlow；V21-07 交付 Match；V21-08 交付顺序 Each、CollectionItem cap
//! 与真实 Item 直接 Consume；V21-09 交付 Loop 的 Retry／Iter 正式推进与 Round 收口。
//! 公开 API 与完整 Root 输出移交仍由后续任务接续。

pub(crate) mod builder;
pub(crate) mod context;
pub(crate) mod data_container;
pub(crate) mod data_ref;
#[allow(dead_code)] // V21-08 交付的内部 Each：当前消费者是验收样本；公开入口与 Loop 由后续任务接续
pub(crate) mod each;
pub(crate) mod flow;
pub(crate) mod identity;
pub(crate) mod internal_error;
#[allow(dead_code)]
// V21-09 交付的内部 Loop：当前消费者是验收样本；公开入口与 Root 移交由后续任务接续
pub(crate) mod loop_orchestrator;
#[allow(dead_code)] // V21-07 交付的内部 Match：当前消费者是验收样本；公开入口由后续任务接续
pub(crate) mod match_orchestrator;
pub(crate) mod node;
pub(crate) mod orchestrator;
pub(crate) mod ref_id;
pub(crate) mod runtime;
pub(crate) mod scope;
pub(crate) mod signature;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod v21_05_tests;
#[cfg(test)]
mod v21_06_tests;
#[cfg(test)]
mod v21_07_tests;
#[cfg(test)]
mod v21_08_tests;
#[cfg(test)]
mod v21_08_tests_cap;
#[cfg(test)]
mod v21_08_tests_lifecycle;
#[cfg(test)]
mod v21_08_tests_shapes;
#[cfg(test)]
mod v21_09_tests;
#[cfg(test)]
mod v21_09_tests_failures;
#[cfg(test)]
mod v21_09_tests_revision;
#[cfg(test)]
mod v21_09_tests_revision2;
