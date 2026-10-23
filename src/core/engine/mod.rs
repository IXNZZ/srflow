//! Workflow Engine v0.1：新的公开契约与真实执行主链（WE-T01～WE-T03）。
//!
//! - [`data`]：Data 声明与标准容器；
//! - [`shape`]：`InputSpec`／`RefShape`／`Ref`／`Query` 的类型映射；
//! - [`node`]：`Node` 声明协议与类型化调用桥 `Callable`；
//! - [`flow`]：同步定义上下文 `Flow::then`／`Flow::chain`／`Flow::choose`；
//! - [`choice`]：Choose 的 `case`／`otherwise` 定义上下文；
//! - [`definition`]：Body／Step、来源登记、词法可见性、静态 Core RefId 与 `build_definition`；
//! - [`adapter`]：类型化 Node Adapter（借用 → Query → await → 类型化登记）；
//! - [`boundary`]：Chain／Choose 边界的 Import／Export／alias 与三路收口；
//! - [`runtime`]：公开 `Runtime::execute` 与 Root owned 提取；
//! - [`error`]：业务错误通道、构建／运行期错误与执行链内部失败。
//!
//! 阶段边界：本模块已交付 Node／Chain／Each／Choose／Retry／Iter 的完整主链与不可变
//! Definition（SPEC 冻结范围已全部落地；旧上层协议已在 WE-T06 B 阶段退出）。

pub(crate) mod adapter;
pub(crate) mod boundary;
pub(crate) mod choice;
pub(crate) mod data;
pub(crate) mod definition;
pub(crate) mod error;
pub(crate) mod flow;
pub(crate) mod identity;
pub(crate) mod node;
#[cfg(test)]
pub(crate) mod observe;
pub(crate) mod runtime;
pub(crate) mod shape;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_each;
#[cfg(test)]
mod tests_each_consume;
#[cfg(test)]
mod tests_each_control;
#[cfg(test)]
mod tests_each_finish;
#[cfg(test)]
mod tests_each_nested;
#[cfg(test)]
mod tests_promote_group;
#[cfg(test)]
mod tests_retry_iter;
#[cfg(test)]
mod tests_retry_iter_arity;
#[cfg(test)]
mod tests_retry_iter_matrix;
#[cfg(test)]
mod tests_retry_iter_nesting;
#[cfg(test)]
mod tests_retry_iter_state;
#[cfg(test)]
mod tests_runtime;
#[cfg(test)]
mod tests_v22_t01;
#[cfg(test)]
mod tests_v22_t02;
#[cfg(test)]
mod tests_v22_t02_repeat;
#[cfg(test)]
mod tests_v22_t03;
#[cfg(test)]
mod tests_we_t06_arity;
#[cfg(test)]
mod tests_we_t06_root_preflight;
#[cfg(test)]
mod tests_we_t06_witnesses;
