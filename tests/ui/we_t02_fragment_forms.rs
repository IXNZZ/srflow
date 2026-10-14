//! WE-T02 B03 正例：公开 `Flow<'n>` 的 Fragment 两种合法形式。
//!
//! 1. 普通函数返回 closure factory（`impl FnOnce(&mut Flow<'n>) -> Ref<O> + 'n`）；
//! 2. 普通函数直接接收 `&mut Flow<'n>`，在 chain closure 内调用。
//!
//! 两种形式都不要求调用者增加 builder lifetime，也不新增第二 lifetime 参数。
//! 由主包真实 rlib 以外部消费者身份编译运行（`--extern srflow=...`）。
//!
//! 期望：编译成功，运行退出码 0，断言 259，stdout 含 `we_t02-positive-ok`。

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn triple(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 3))
}

struct AddOne;
impl Node for AddOne {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

/// Fragment 形式一：普通函数返回 closure factory，按普通 Rust lifetime 描述
/// 其 Node 依赖的借用寿命。
fn fragment<'n>(
    node: &'n AddOne,
    input: Ref<Amount>,
) -> impl FnOnce(&mut Flow<'n>) -> Ref<Amount> + 'n {
    move |sub| sub.then(node, input)
}

/// Fragment 形式二：普通函数接收 `&mut Flow<'n>`，可在 chain closure 内直接调用。
fn grow<'n>(sub: &mut Flow<'n>, node: &'n AddOne, input: Ref<Amount>) -> Ref<Amount> {
    let doubled = sub.then(double, input);
    sub.then(node, doubled)
}

fn main() {
    let output = block_on(async {
        let runtime = Runtime::new();
        let add = AddOne;
        runtime
            .execute(
                |flow, amount| {
                    // 21 -> double -> 42
                    let doubled = flow.then(double, amount);
                    // 42 -> (chain via closure factory: double + 1) -> 43
                    let plus_one = flow.chain(fragment(&add, doubled));
                    // 43 -> triple -> 129
                    let tripled = flow.then(triple, plus_one);
                    // 129 -> (chain via `&mut Flow<'n>` function: double + 1) -> 259
                    flow.chain(|sub| grow(sub, &add, tripled))
                },
                Amount(21),
            )
            .await
    })
    .unwrap();
    assert_eq!(output.0, 259, "closure factory + &mut Flow fragment forms");

    println!("we_t02-positive-ok");
}
