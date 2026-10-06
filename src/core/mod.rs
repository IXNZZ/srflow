//! SRFlow Core Runtime 的内部模块。
//!
//! 这里的内容不对外导出，业务侧无法访问 DataContainer、ID 或存储操作。V21-01 交付
//! Execution 身份、异构 owned Data 存储与短期只读借用；V21-02 交付 Scope registry、
//! Import／Export 与同步退出边界；V21-03 交付 `CollectorId` 身份、控制状态登记与状态
//! 来源导入、Promote、受控 pending 回收、collector 建构区与 ItemScope 直接 Consume；
//! V21-04 交付一次 Execution 的内部调用设施（单 Context、Invocation frame 与退出
//! guard、共享重借用下的异步借用边界及执行错误／取消清理）；V21-05 交付 DataRef／
//! Signature、业务 Node 与 Orchestrator 协议、强类型异构 CallSite 及真实 Context 双路径
//! 分派；V21-06 交付完整内部 Flow 与 SubFlow；V21-07 交付 Match；V21-08 交付顺序 Each、
//! CollectionItem cap 与真实 Item 直接 Consume；V21-09 交付 Loop 的 Retry／Iter 正式
//! 推进与 Round 收口；V21-10 交付非 `cfg(test)` 的内部 Root 入口 `Runtime::execute`
//! （owned Root 输入、真实 Orchestrator body 与完整输出移交）；V21-11 完成跨控制器
//! 整合与故障验证。V21-12 起，受支持能力经 crate 根的公开 facade（`crate::api`）
//! 导出，本模块内部项仍不对外可达。

pub(crate) mod builder;
pub(crate) mod context;
pub(crate) mod data_container;
pub(crate) mod data_ref;
pub(crate) mod each;
pub(crate) mod flow;
pub(crate) mod identity;
pub(crate) mod internal_error;
pub(crate) mod loop_orchestrator;
pub(crate) mod match_orchestrator;
pub(crate) mod node;
pub(crate) mod orchestrator;
pub(crate) mod ref_id;
pub(crate) mod root_signature;
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
#[cfg(test)]
mod v21_10_tests;
#[cfg(test)]
mod v21_10_tests_defence;
#[cfg(test)]
mod v21_11_tests;
#[cfg(test)]
mod v21_11_tests_cancel;
#[cfg(test)]
mod v21_11_tests_combos;
#[cfg(test)]
mod v21_11_tests_failures;
