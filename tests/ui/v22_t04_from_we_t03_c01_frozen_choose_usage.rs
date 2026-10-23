//! V22-T03 承接 WE-T03 C01 正例：冻结的 Choose 用法在**独立消费者**中真实运行。
//!
//! 冻结形态（SPEC §9）：
//!
//! ```text
//! flow.choose(selector, |choice| {
//!     choice.case(K, |branch| branch.then_with_output(&node, input));
//!     …
//!     choice.otherwise(|branch| branch.then_with_output(&node, input));
//! })
//! ```
//!
//! 本夹具以真实主包 rlib 编译（`--extern srflow=target/debug/libsrflow.rlib`），不装配内部
//! 模块、不使用 `#[path]`，因此是外部消费者视角。覆盖：
//!
//! 1. 冻结用法：`case` / `case` / `otherwise`，只执行选中分支；
//! 2. 嵌套：分支定义内部再 `chain`，并在其中发起内层 `choose`；
//! 3. Fragment：普通函数返回闭包（`impl FnOnce(&mut Flow<'n>) -> Ref<Amount> + 'n`）
//!    直接作为分支定义。
//!
//! T03 契约迁移：旧两参 `flow.then(node, refs)` 改为显式连接入口
//! `flow.then_with_output(node, refs)`；Root 闭包固定返回 `()`，业务输出改由
//! `Flow::output_with` 声明（owned `Out` 由 `let output: Amount` 的局部类型给出）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `t03-c01-frozen-choose-usage-ok`。

use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

/// T03 夹具自带的极小 executor：本版引擎的单次 Root 执行在有限次 poll 内完成，
/// 不需要真实唤醒。夹具不引用 `futures`（驱动只提供主包 rlib）。
fn drive<F: core::future::Future>(future: F) -> F::Output {
    use core::task::{Context, Poll, Waker};
    let mut future = Box::pin(future);
    let mut pending = 0usize;
    loop {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                pending += 1;
                assert!(pending < 64, "future is not making progress");
            }
        }
    }
}

#[derive(Data)]
struct Seed(u32);

#[derive(Data)]
struct Amount(u32);

/// 匹配键：值配置，不作为业务输入登记。
#[derive(Data, PartialEq)]
struct Route(u32);

fn route(n: u32) -> Route {
    Route(n)
}

async fn route_of(query: Query<&Seed>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 3))
}

async fn route_of_amount(query: Query<&Amount>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 3))
}

async fn seed_reader(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn alpha(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

async fn beta(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 2))
}

async fn fallback(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 10))
}

struct Stepper;

impl Node for Stepper {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Amount>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

/// Fragment：普通函数返回引用 Node 的闭包，可直接作为 `case`／`otherwise` 的分支定义。
fn fragment<'n>(
    node: &'n Stepper,
    input: Ref<Amount>,
) -> impl FnOnce(&mut Flow<'n>) + 'n {
    move |branch| {
        let out_refs = branch.then_with_output(node, input);
        branch.output_with(out_refs);
    }
}

fn main() {
    let runtime = Runtime::new();

    // 1) 冻结用法：seed % 3 选 0/1，否则 otherwise；只执行选中分支。
    for (value, expected) in [(0u32, 1u32), (1u32, 3u32), (2u32, 12u32)] {
        let output: Amount = drive(runtime.execute(
            |flow, seed| {
                let selector = flow.then_with_output(route_of, seed);
                let picked = flow.choose(selector, |choice| {
                    choice.case(route(0), |branch| branch.then_with_output(alpha, seed));
                    choice.case(route(1), |branch| branch.then_with_output(beta, seed));
                    choice.otherwise(|branch| branch.then_with_output(fallback, seed));
                });
                flow.output_with(picked);
            },
            Seed(value),
        ))
        .expect("frozen choose run");
        assert_eq!(output.0, expected, "case/case/otherwise selects one branch");
    }

    // 2) 嵌套：分支内 `chain` + 内层 `choose`，内层分支以 Fragment 定义。
    let step = Stepper;
    let output: Amount = drive(runtime.execute(
        |flow, seed| {
            let selector = flow.then_with_output(route_of, seed);
            let outer = flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    // 分支内 chain：登记一个 child Body 并取回 parent 可见的新 Ref。
                    let inner: Ref<Amount> = branch.chain_output(|sub|{
                        let out_refs = sub.then_with_output(seed_reader, seed);
                        sub.output_with(out_refs);
                    });
                    let inner_route = branch.then_with_output(route_of_amount, inner);
                    // 分支内再 choose：内层分支同样支持 Fragment 与 otherwise。
                    let picked = branch.choose(inner_route, |inner_choice| {
                        inner_choice.case(route(2), |b| {
                            let refs: Ref<Amount> = b.chain_output(fragment(&step, inner));
                            refs
                        });
                        inner_choice.otherwise(|deep| deep.then_with_output(fallback, seed));
                    });
                    branch.chain_output(fragment(&step, picked))
                });
                choice.otherwise(|branch| branch.then_with_output(fallback, seed));
            });
            flow.output_with(outer);
        },
        Seed(10),
    ))
    .expect("nested choose run");
    // seed=10 → selector=Route(1) → 命中 case；inner=seed_reader=11；inner selector=11%3=Route(2)
    // → 内层 fragment(Stepper)=12 → 外层 fragment(Stepper)=13。
    assert_eq!(output.0, 13, "nested chain + inner choose + fragment branches");

    println!("t03-c01-frozen-choose-usage-ok");
}
