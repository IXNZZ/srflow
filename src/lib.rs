//! SRFlow Core Runtime 的内部实现骨架。
//!
//! 当前 crate 不提供任何公开能力：全部内容位于非公开的 `core` 模块内。
//! V21-01 交付 Execution 身份、异构 Data 存储与短期借用；Scope、Node、
//! Orchestrator 与 Root 边界由后续任务接入。

#![forbid(unsafe_code)]

mod core;
