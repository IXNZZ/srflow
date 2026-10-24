//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_unit_iter_state.rs`）：无状态 `()` Iter 不支持。
//!
//! 原目的（X24／C13／C15）：SPEC §3.2「无状态 `()` Iter 不支持」——Iter State 形状要求单 Ref
//! 或 2～16 flat Ref tuple。`flow.iter((), …)` 的 `S = ()` 不实现 `StateShape`，必须由 rustc 拒绝。
//!
//! 迁移手法：旧闭包尾表达式就是 `flow.iter(..) -> ()`，本夹具改为「求值一次后显式声明 unit
//! 输出」的标准形式（`flow.iter(...);` 后 `flow.output_with(());`），并不改动 `iter` 调用本身；
//! 旧的 `()` Root 输入与两份闭包参数原样保留。`iter` 的 StateShape 拒绝范围不变。
//!
//! 归因：**rustc 编译拒绝**，不是 Definition 构建期拒绝。
//!
//! 期望主诊断：E0277（`(): StateShape` 未满足）。

use srflow::{Flow, Runtime};

fn main() {
    let runtime = Runtime::new();
    let _ = runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, ()| {
            flow.iter((), 3, |round| {
                let _ = round;
                round.next_with(());
            });
            flow.output_with(());
        },
        (),
    );
}
