//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_p01_root_linear.rs`）：Root 线性 owned 流。
//!
//! 原目的（X24/S01／C13／C15）：多输入 Root、后续 Node 读取前一步产物、无数据依赖的 unit 步骤
//! 仍按定义顺序执行、tuple 输出，以及两次 `execute` 各自独立构建 Definition；断言
//! `(total, doubled) = (43, 42)` 且两次 execute 各执行一次 unit 步骤。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)` 改为 `flow.then_with_output(node, refs)`；Root 闭包
//! 改为固定返回 `()`，原 tuple 尾表达式求值一次后交给 `flow.output_with`，并以
//! `execute::<_, (Total, Amount), _>` 补齐显式 owned `Out`。断言与调用顺序不变。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-positive-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Bonus(u32);

#[derive(Data)]
struct Total(u32);

/// unit 步骤的业务计数：证明「没有数据依赖的步骤仍执行一次」。
static AUDITED: AtomicUsize = AtomicUsize::new(0);

/// 单输入函数 Node：读取 Root 输入，产生新的 owned 值。
async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

/// unit 输出 Node：不产生业务槽，但真实执行一次（SPEC §5.3）。
async fn audit(query: Query<&Amount>) -> Result<(), BodyError> {
    assert!(query.get().0 > 0, "the audited value stays readable");
    AUDITED.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

/// 多输入函数 Node：读取前一步产物与另一个 Root 输入。
async fn combine(query: Query<(&Amount, &Bonus)>) -> Result<Total, BodyError> {
    let (amount, bonus) = query.get();
    Ok(Total(amount.0 + bonus.0))
}

fn main() {
    let runtime = Runtime::new();

    let run_once = ||
        block_on(runtime.execute::<_, (Total, Amount), _>(
            |flow: &mut Flow<'_>, (amount, bonus): (Ref<Amount>, Ref<Bonus>)| {
                let doubled = flow.then_with_output(double, amount);
                // 无数据依赖的 unit 步骤：仍登记为真实 Step，并按定义顺序执行一次。
                let _: () = flow.then_with_output(audit, doubled);
                let total = flow.then_with_output(combine, (doubled, bonus));
                let out_refs: (Ref<Total>, Ref<Amount>) = (total, doubled);
                flow.output_with(out_refs);
            },
            (Amount(21), Bonus(1)),
        ))
        .expect("root linear flow runs");

    let (total, doubled) = run_once();
    assert_eq!((total.0, doubled.0), (43, 42), "多输入与 tuple 输出按声明顺序交付");

    // 第二次 execute 独立构建 Definition，结果一致（不复用上一次的 Ref）。
    let (total_again, doubled_again) = run_once();
    assert_eq!((total_again.0, doubled_again.0), (43, 42));

    assert_eq!(
        AUDITED.load(Ordering::SeqCst),
        2,
        "每次 execute 的 unit 步骤各执行一次"
    );

    println!("we_t06 p01 total={} doubled={}", total.0, doubled.0);
    println!("we_t06-positive-ok");
}
