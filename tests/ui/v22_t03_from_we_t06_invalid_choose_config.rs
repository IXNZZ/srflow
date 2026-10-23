//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_invalid_choose_config.rs`）：Choose 配置非法。
//!
//! 原目的（X24/X05／C13／C15）：三个场景都在 **Definition 构建期**被拒绝（`RunError::Definition`），
//! 业务 Node 零调用：空配置（`EmptyChoose`，诊断含 "neither a case nor an otherwise"）、重复 case 值
//! （`DuplicateCase`，"repeats a case value"）、第二个 otherwise（`DuplicateOtherwise`，"second otherwise"）。
//!
//! 判定与归因：**Definition 构建期拒绝**，不是 rustc 拒绝——`case(0)` 两次在类型层面完全合法。
//!
//! 迁移手法：Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out`（空配置场景为 `()`，
//! 其余为 `Amount`）；旧两参 `flow.then(node, refs)`（含 selector 与分支内）改为 `then_with_output`。
//! 三个场景的配置错误与零调用断言不变。
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
struct Amount(u32);

#[derive(Data)]
struct Route(u32);

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Route(query.get().value % 2))
}

async fn amount_of(query: Query<&Seed>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().value))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);

    // 场景 1：没有任何分支。
    let empty = block_on(runtime.execute::<_, (), _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked: () = flow.choose(selector, |_choice| {});
            flow.output_with(picked);
        },
        Seed { value: 0 },
    ))
    .err()
    .expect("a choose with neither case nor otherwise is rejected");
    assert!(
        matches!(empty, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {empty}"
    );
    assert!(
        empty.to_string().contains("neither a case nor an otherwise"),
        "诊断应指出空配置：{empty}"
    );

    // 场景 2：同一匹配值登记两次。
    let duplicate_case = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| branch.then_with_output(&amount_of, seed));
                choice.case(Route(0), |branch| branch.then_with_output(&amount_of, seed));
            });
            flow.output_with(picked);
        },
        Seed { value: 0 },
    ))
    .err()
    .expect("a repeated case value is rejected");
    assert!(
        matches!(duplicate_case, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {duplicate_case}"
    );
    assert!(
        duplicate_case.to_string().contains("repeats a case value"),
        "诊断应指出重复匹配值：{duplicate_case}"
    );

    // 场景 3：第二个 otherwise。
    let duplicate_otherwise = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let selector = flow.then_with_output(route_of, seed);
            let picked = flow.choose(selector, |choice| {
                choice.case(Route(0), |branch| branch.then_with_output(&amount_of, seed));
                choice.otherwise(|branch| branch.then_with_output(&amount_of, seed));
                choice.otherwise(|branch| branch.then_with_output(&amount_of, seed));
            });
            flow.output_with(picked);
        },
        Seed { value: 0 },
    ))
    .err()
    .expect("a second otherwise is rejected");
    assert!(
        matches!(duplicate_otherwise, RunError::Definition(_)),
        "构建期拒绝应为 RunError::Definition，实得 {duplicate_otherwise}"
    );
    assert!(
        duplicate_otherwise
            .to_string()
            .contains("second otherwise"),
        "诊断应指出重复 otherwise：{duplicate_otherwise}"
    );

    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        0,
        "被拒绝的 Definition 不执行任何业务 Node"
    );

    println!("we_t06 empty choose:      {empty}");
    println!("we_t06 duplicate case:    {duplicate_case}");
    println!("we_t06 duplicate otherw:  {duplicate_otherwise}");
    println!("we_t06-build-reject-ok");
}
