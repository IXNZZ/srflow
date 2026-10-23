//! V22-T03 承接 WE-T03 C22 负例：公开面不能制造**无来源的 `Choice`**。
//!
//! `Choice` 由 `Flow::choose` 建立并在同一次同步定义调用内消费；它的构造入口
//! （`Choice::new`）是 crate 内可见的 associated function，字段全部私有。外部消费者既不能
//! 命名/构造一个 `Choice`，也就无法伪造分支配置来伪造执行路径（SPEC §9；任务书 §C22）。
//!
//! 这里刻意**不**用 `doc(hidden)`、不可命名类型或缺 `Default` 来“证明”访问控制：目标
//! 诊断是真实的可见性错误。本夹具不经过 Root 定义闭包，因此不需要 T03 契约迁移。
//!
//! 期望主诊断：E0624（associated function `new` 为私有）。

use srflow::{Choice, Data};

#[derive(Data, PartialEq)]
struct Route(u32);

fn probe_choice<'n>() -> Choice<'n, Route, ()> {
    // 期望主诊断：E0624——`Choice::new` 不是公开入口。
    Choice::new(todo!(), todo!(), 0, todo!())
}

fn main() {
    let _ = probe_choice::<'_>;
    let _ = Route(1);
}
