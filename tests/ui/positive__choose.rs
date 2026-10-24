//! V22-T03 夹具迁移（源自 WE_T06 `we_t06_p05_choose.rs`）：Choose（SPEC §9）。
//!
//! 原目的（X24/S05／C13／C15）：同一 `execute` 入口、`PartialEq` 最小键约束、所有分支构建期
//! 登记但只执行选中一支（分支计数各 1／0／0）、`otherwise` 回退，以及无匹配且无 `otherwise`
//! 返回 `RunError::NoMatchingCase` 且不执行分支 Node。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)`（含 case／otherwise 分支内的 `branch.then(..)`）
//! 改为 `then_with_output`；只有 Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned
//! `Out = Report`。Choose 的分支协议与全部计数断言不变。
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Seed {
    value: u32,
}

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Report(u32);

/// 选择键：只声明 `Data` 并手写 `PartialEq`（不派生 `Eq`／`Hash`／`Clone`／`Copy`）。
#[derive(Data)]
struct Route(u32);

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

static DOUBLED: AtomicUsize = AtomicUsize::new(0);
static TRIPLED: AtomicUsize = AtomicUsize::new(0);
static FELL_BACK: AtomicUsize = AtomicUsize::new(0);

/// 由上游业务值产生 selector（`Ref<Route>`）。
async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    Ok(Route(query.get().value % 2))
}

async fn double(query: Query<&Seed>) -> Result<Amount, BodyError> {
    DOUBLED.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().value * 2))
}

async fn triple(query: Query<&Seed>) -> Result<Amount, BodyError> {
    TRIPLED.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().value * 3))
}

async fn fallback(query: Query<&Seed>) -> Result<Amount, BodyError> {
    FELL_BACK.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().value + 100))
}

async fn to_report(query: Query<&Amount>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0))
}

fn main() {
    let runtime = Runtime::new();

    // 1) case 命中（value=2 → code 0 → double）：只有一支执行。
    let matched = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked: Ref<Amount> =
                flow.choose_with_output::<Route, Ref<Amount>, _>(selector, |choice| {
                    choice.case(Route(0), |branch| {
                        let value = branch.then_with_output(double, seed);
                        branch.output_with(value);
                    });
                    choice.case(Route(1), |branch| {
                        let value = branch.then_with_output(triple, seed);
                        branch.output_with(value);
                    });
                    choice.otherwise(|branch| {
                        let value = branch.then_with_output(fallback, seed);
                        branch.output_with(value);
                    });
                });
            let out_refs = flow.then_with_output(to_report, picked);
            flow.output_with(out_refs);
        },
        Seed { value: 2 },
    ))
    .expect("matched case runs");
    assert_eq!(matched.0, 4);
    assert_eq!(
        (
            DOUBLED.load(Ordering::SeqCst),
            TRIPLED.load(Ordering::SeqCst),
            FELL_BACK.load(Ordering::SeqCst)
        ),
        (1, 0, 0),
        "只执行选中的分支"
    );

    // 2) case 命中另一支（value=3 → code 1 → triple）。
    let other_branch = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked: Ref<Amount> =
                flow.choose_with_output::<Route, Ref<Amount>, _>(selector, |choice| {
                    choice.case(Route(0), |branch| {
                        let value = branch.then_with_output(double, seed);
                        branch.output_with(value);
                    });
                    choice.case(Route(1), |branch| {
                        let value = branch.then_with_output(triple, seed);
                        branch.output_with(value);
                    });
                    choice.otherwise(|branch| {
                        let value = branch.then_with_output(fallback, seed);
                        branch.output_with(value);
                    });
                });
            let out_refs = flow.then_with_output(to_report, picked);
            flow.output_with(out_refs);
        },
        Seed { value: 3 },
    ))
    .expect("second case runs");
    assert_eq!(other_branch.0, 9);
    assert_eq!(
        (
            DOUBLED.load(Ordering::SeqCst),
            TRIPLED.load(Ordering::SeqCst),
            FELL_BACK.load(Ordering::SeqCst)
        ),
        (1, 1, 0)
    );

    // 3) otherwise 回退：只登记不匹配的 case，value=5 → code 1。
    let fell_back = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked: Ref<Amount> =
                flow.choose_with_output::<Route, Ref<Amount>, _>(selector, |choice| {
                    choice.case(Route(0), |branch| {
                        let value = branch.then_with_output(double, seed);
                        branch.output_with(value);
                    });
                    choice.otherwise(|branch| {
                        let value = branch.then_with_output(fallback, seed);
                        branch.output_with(value);
                    });
                });
            let out_refs = flow.then_with_output(to_report, picked);
            flow.output_with(out_refs);
        },
        Seed { value: 5 },
    ))
    .expect("otherwise fallback runs");
    assert_eq!(fell_back.0, 105);
    assert_eq!(FELL_BACK.load(Ordering::SeqCst), 1);

    // 4) 无匹配且无 otherwise → RunError::NoMatchingCase。
    let no_match = block_on(runtime.execute::<_, Report, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked: Ref<Amount> =
                flow.choose_with_output::<Route, Ref<Amount>, _>(selector, |choice| {
                    choice.case(Route(9), |branch| {
                        let value = branch.then_with_output(double, seed);
                        branch.output_with(value);
                    });
                });
            let out_refs = flow.then_with_output(to_report, picked);
            flow.output_with(out_refs);
        },
        Seed { value: 5 },
    ));
    match no_match {
        Err(RunError::NoMatchingCase) => {}
        Err(other) => panic!("expected NoMatchingCase, got {other}"),
        Ok(_) => panic!("an unmatched choose must not produce a normal output"),
    }
    assert_eq!(
        DOUBLED.load(Ordering::SeqCst),
        1,
        "无匹配不执行任何分支 Node"
    );

    println!("we_t06 p05 matched={} fallback={}", matched.0, fell_back.0);
    println!("we_t06-positive-ok");
}
