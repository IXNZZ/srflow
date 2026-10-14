//! WE-T03 C01 正例：冻结的 Choose 用法在**独立消费者**中真实运行。
//!
//! 冻结形态（SPEC §9）：
//!
//! ```text
//! flow.choose(route, |choice| {
//!     choice.case(K, |branch| branch.then(&node, data));
//!     …
//!     choice.otherwise(|branch| branch.then(&node, data));
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
//! `branch` 就是 `&mut Flow<'n>`，没有内部 Signature 类型、没有第二个 lifetime 参数，
//! 公开面只有单一 `choose` 方法名（不按 arity 分类）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we_t03-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

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
) -> impl FnOnce(&mut Flow<'n>) -> Ref<Amount> + 'n {
    move |branch| branch.then(node, input)
}

fn main() {
    let runtime = Runtime::new();

    // 1) 冻结用法：seed % 3 选 0/1，否则 otherwise；只执行选中分支。
    for (value, expected) in [(0u32, 1u32), (1u32, 3u32), (2u32, 12u32)] {
        let output = block_on(runtime.execute(
            |flow, seed| {
                let selector = flow.then(route_of, seed);
                flow.choose(selector, |choice| {
                    choice.case(route(0), |branch| branch.then(alpha, seed));
                    choice.case(route(1), |branch| branch.then(beta, seed));
                    choice.otherwise(|branch| branch.then(fallback, seed));
                })
            },
            Seed(value),
        ))
        .expect("frozen choose run");
        assert_eq!(output.0, expected, "case/case/otherwise selects one branch");
    }

    // 2) 嵌套：分支内 `chain` + 内层 `choose`，内层分支以 Fragment 定义。
    let step = Stepper;
    let output = block_on(runtime.execute(
        |flow, seed| {
            let selector = flow.then(route_of, seed);
            flow.choose(selector, |choice| {
                choice.case(route(1), |branch| {
                    // 分支内 chain：登记一个 child Body 并取回 parent 可见的新 Ref。
                    let inner = branch.chain(|sub| sub.then(seed_reader, seed));
                    let inner_route = branch.then(route_of_amount, inner);
                    // 分支内再 choose：内层分支同样支持 Fragment 与 otherwise。
                    let picked = branch.choose(inner_route, |inner_choice| {
                        inner_choice.case(route(2), fragment(&step, inner));
                        inner_choice.otherwise(|deep| deep.then(fallback, seed));
                    });
                    branch.chain(fragment(&step, picked))
                });
                choice.otherwise(|branch| branch.then(fallback, seed));
            })
        },
        Seed(10),
    ))
    .expect("nested choose run");
    // seed=10 → selector=Route(1) → 命中 case；inner=seed_reader=11；inner selector=11%3=Route(2)
    // → 内层 fragment(Stepper)=12 → 外层 fragment(Stepper)=13。
    assert_eq!(output.0, 13, "nested chain + inner choose + fragment branches");

    println!("we_t03-positive-ok");
}
