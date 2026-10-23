//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_unselected_branch_source.rs`）：未选中分支的非法来源。
//!
//! 原目的（X24/X05／C13／C15）：Choose 的**所有** case 定义闭包在构建期同步执行并登记；任何
//! 分支的静态来源非法都在 Definition 验证阶段拒绝，与运行期是否选中该分支无关（SPEC §9）；
//! 本夹具分支 2 使用 foreign Ref，运行期会命中分支 1，仍在任何业务 Node 之前被 Definition 拒绝。
//!
//! 判定与归因：**Definition 构建期拒绝**（`RunError::Definition`），不是 rustc 拒绝。
//!
//! 迁移手法：两处 Root 闭包改为固定返回 `()` 并经 `output_with` 声明（`Mark`／`Amount`），旧两参
//! `flow.then(node, refs)`（含分支闭包内）改为 `then_with_output`；纤维与断言不变。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t06-build-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Seed {
    value: u32,
}

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Mark(u32);

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Route(u32);

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn touch(query: Query<&Item>) -> Result<Mark, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0))
}

async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    Ok(Route(query.get().value % 2))
}

/// 分支 1（运行期会被选中）使用的合法来源。
async fn double(query: Query<&Seed>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().value * 2))
}

/// 分支 2（运行期不会被选中）使用 foreign 来源。
async fn review(query: Query<&Mark>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    // 第一次执行：泄漏一个 Ref<Mark> 作为 foreign 来源。
    let mut leaked: Option<Ref<Mark>> = None;
    let first = block_on(runtime.execute::<_, Mark, _>(
        |flow: &mut Flow<'_>, item: Ref<Item>| {
            let mark = flow.then_with_output(&touch, item);
            leaked = Some(mark);
            flow.output_with(mark);
        },
        Item(11),
    ))
    .expect("first execution succeeds");
    assert_eq!(first.0, 11);
    let foreign = leaked.expect("leaked mark ref");
    let calls_after_first = CALLS.load(Ordering::SeqCst);

    // 第二次执行：Seed.value = 0 → 运行期命中 case Route(0)；分支 2 的非法来源仍在构建期被拒。
    let error = block_on(runtime.execute::<_, Amount, _>(
        move |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            flow.choose_with(selector, |choice| {
                choice.case(Route(0), |branch| {
                    let value = branch.then_with_output(double, seed);
                    branch.output_with(value);
                });
                choice.case(Route(1), |branch| {
                    let value = branch.then_with_output(review, foreign);
                    branch.output_with(value);
                });
            });
            let out = flow.then_with_output(double, seed);
            flow.output_with(out);
        },
        Seed { value: 0 },
    ))
    .err()
    .expect("an unselected branch with an illegal source is rejected at build time");
    assert!(
        matches!(error, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {error}"
    );
    assert!(
        error.to_string().contains("another definition"),
        "诊断应指出非法来源属于另一 Definition：{error}"
    );
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        calls_after_first,
        "拒绝发生在任何业务 Node 之前（含被选中分支）"
    );

    println!("we_t06 unselected branch: {error}");
    println!("we_t06-build-reject-ok");
}
