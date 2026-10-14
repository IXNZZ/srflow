//! WE-T03 C06 负例（Definition 构建期拒绝）：上一次执行**泄漏的 Ref** 不可复用。
//!
//! 外部唯一的合法 `Ref` 来源是真实执行：`Ref` 无构造入口（见 C22），能把 `Ref` 带出定义
//! 闭包的只有执行期产生的合法位置。把这种跨执行（foreign）的 `Ref` 用于另一次
//! `execute` 的 Definition 时，必须在**任何业务 Node／RootExecution 开始前**被拒绝，
//! 且未选分支也必须完成来源校验（SPEC §9；任务书 §4.2）。
//!
//! 本夹具真实编译并运行，断言两个场景都返回 `RunError::Definition`：
//!
//! - 场景 A：foreign `Ref` 直接作为 `choose` 的 **selector**；
//! - 场景 B：foreign `Ref` 只出现在**未选分支**（selector 值不会命中该 case）。
//!
//! 同时断言业务 Node 计数在两次拒绝后保持不变（验证失败无 Node／Root）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t03-reject-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, RunError, Runtime};

#[derive(Data)]
struct Amount(u32);

#[derive(Data, PartialEq)]
struct Route(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn counted(query: Query<&Amount>) -> Result<Amount, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 + 1))
}

async fn route_of_amount(query: Query<&Amount>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 3))
}

fn main() {
    let runtime = Runtime::new();

    // 第一次执行：把 Root 输入 Ref 与执行期产生的 selector Ref 带出定义闭包。
    let mut leaked_amount: Option<Ref<Amount>> = None;
    let mut leaked_route: Option<Ref<Route>> = None;
    let first = block_on(runtime.execute(
        |flow, amount| {
            leaked_amount = Some(amount);
            let route = flow.then(route_of_amount, amount);
            leaked_route = Some(route);
            flow.choose(route, |choice| {
                choice.case(Route(1), |branch| branch.then(counted, amount));
                choice.otherwise(|branch| branch.then(counted, amount));
            })
        },
        Amount(4),
    ))
    .expect("first execution succeeds");
    assert_eq!(first.0, 5);
    let foreign_amount = leaked_amount.expect("root ref leaked");
    let foreign_route = leaked_route.expect("selector ref leaked");
    assert_eq!(CALLS.load(Ordering::SeqCst), 1, "first execution ran one Node");

    // 场景 A：foreign Ref 作为 selector → 构建期拒绝。
    let err_a = block_on(runtime.execute(
        move |flow, amount| {
            let _ = flow.then(counted, amount);
            flow.choose(foreign_route, |choice| {
                choice.case(Route(0), |branch| branch.then(counted, amount));
                choice.otherwise(|branch| branch.then(counted, amount));
            })
        },
        Amount(1),
    ))
    .err()
    .expect("foreign selector is rejected before any Node runs");
    assert!(matches!(err_a, RunError::Definition(_)), "{err_a}");

    // 场景 B：foreign Ref 只出现在未选分支（Amount(2) → Route(2) → otherwise）→ 仍被拒绝。
    let err_b = block_on(runtime.execute(
        move |flow, amount| {
            let selector = flow.then(route_of_amount, amount);
            flow.choose(selector, |choice| {
                choice.case(Route(1), |branch| branch.then(counted, foreign_amount));
                choice.otherwise(|branch| branch.then(counted, amount));
            })
        },
        Amount(2),
    ))
    .err()
    .expect("foreign Ref in an unselected branch is rejected");
    assert!(matches!(err_b, RunError::Definition(_)), "{err_b}");

    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        1,
        "rejected definitions started no business Node"
    );
    println!("c06 foreign selector: {err_a}");
    println!("c06 unselected branch: {err_b}");
    println!("we_t03-reject-ok");
}
