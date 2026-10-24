//! T05 迁移自 `v22_t03_from_we_t04_e32_private_constructors_unreachable.rs`（承接 WE-T04 E32 负例）。
//!
//! V22-T05 迁移自 `we_t04_e32_private_constructors_unreachable.rs`（WE-T04 E32 负例，编译期拒绝）：
//! Choice／私有构造入口不可从 crate 外部构造。
//!
//! 公开面不能凭空制造内部定义上下文：`Choice::new` 与 `Flow::open` 都是 crate 内可见的
//! associated function（字段／句柄全部私有）。外部消费者既不能命名 `Builder`／`BodyId`，
//! 也不能调用这些构造入口，因此无法伪造分支配置或第二执行路径（任务书 §8、E32；SPEC §12）。
//!
//! T05 契约迁移：`Choice` 只带两个泛型 `Choice<'n, K>`（旧的第三泛型 `O = ()` 已移除）；
//! `Choice::new` 仍是私有关联函数，但参数元数改为 3（builder／body／step）。
//!
//! 内部 state 许可类型（`ControlStateId`／`StateImportSlot`／`RoundCollectPermit` 等）位于
//! 私有模块，外部连**命名**都做不到（导入即 E0432／E0603，属装配失败而非“可回填诊断码的
//! 目标拒绝”，故按 T03 的做法不在此编码）；这里用真实可见性错误证明“私有构造不可达”。
//!
//! 期望主诊断：E0624（associated function 为私有）。
use srflow::{Choice, Data, Flow};

#[derive(Data, PartialEq)]
struct Route(u32);

#[allow(dead_code)] // 定义体在类型检查阶段即产生目标诊断，与是否被调用无关
fn probe_choice<'n>() -> Choice<'n, Route> {
    // 期望诊断：E0624——`Choice::new` 不是公开入口。
    Choice::new(todo!(), todo!(), 0)
}

#[allow(dead_code)]
fn probe_flow<'n>() {
    // 期望诊断：E0624——`Flow::open` 是 crate 内部构造路径。
    let _ = Flow::<'n>::open(todo!(), todo!());
}

fn main() {
    let _ = Route(1);
}
