//! T05 迁移自 `v22_t03_from_we_t03_c22_choice_not_constructible.rs`（承接 WE-T03 C22 负例）。
//!
//! V22-T05 承接 WE-T03 C22 负例：公开面不能制造**无来源的 `Choice`**。
//!
//! `Choice` 由 `Flow::choose_with`／`choose_with_output` 建立并在同一次同步定义调用内消费；
//! 它的构造入口（`Choice::new`）是 crate 内可见的 associated function（三参：
//! builder／body／step），字段全部私有。外部消费者既不能命名/构造一个 `Choice`，也就无法
//! 伪造分支配置来伪造执行路径（SPEC §9；任务书 §C22）。
//!
//! T05 契约迁移：`Choice` 只带两个泛型 `Choice<'n, K>`（旧的第三泛型 `O = ()` 已移除）；
//! `Choice::new` 仍是私有关联函数，但参数元数改为 3。本夹具不经过 Root 定义闭包，因此
//! 不需要 Choose/Each 调用点迁移。
//!
//! 期望主诊断：E0624（associated function `new` 为私有）。

use srflow::{Choice, Data};

#[derive(Data, PartialEq)]
struct Route(u32);

fn probe_choice<'n>() -> Choice<'n, Route> {
    // 期望主诊断：E0624——`Choice::new` 不是公开入口。
    Choice::new(todo!(), todo!(), 0)
}

fn main() {
    let _ = probe_choice::<'_>;
    let _ = Route(1);
}
