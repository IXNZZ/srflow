//! T03 迁移自 `tests/ui/we_t02_fragment_forms.rs`（WE-T02 B03 正例）：公开 `Flow<'n>` 的
//! Fragment 两种合法形式在 T03 契约下仍然成立。
//!
//! 1. 普通函数返回 closure factory（`impl FnOnce(&mut Flow<'n>) -> Ref<O> + 'n`）；
//! 2. 普通函数直接接收 `&mut Flow<'n>`，在 chain closure 内调用。
//!
//! 两种形式都不要求调用者增加 builder lifetime，也不新增第二 lifetime 参数。
//!
//! T03 迁移点（只改 Root 输出声明与显式连接名）：
//! - fragment／grow 内的显式连接由旧两参 `then(node, refs)` 更名为 `then_with_output(node, refs)`；
//! - Root 定义闭包固定返回 `()`，业务输出改由 `flow.output_with(refs)` 声明；
//! - owned Out 由 `execute::<_, Amount, _>` 显式给出。
//! 两个 child 尾表达式（closure factory／`&mut Flow` 函数）仍按原契约返回 `Ref<Amount>`，
//! 不做递归改写；接线顺序与断言 259 逐字保留。
//!
//! 由主包真实 rlib 以外部消费者身份编译运行；文件内联最小 executor 取代原 `futures::executor::block_on`
//! （驱动语义相同），夹具不依赖 dev-dependency。
//!
//! 期望：编译成功，运行退出码 0，断言 259，stdout 含 `we_t02-positive-ok`。

use std::future::Future;
use std::task::{Context, Poll, Waker};

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
    move |sub| sub.then_with_output(node, input)
}

/// Fragment 形式二：普通函数接收 `&mut Flow<'n>`，可在 chain closure 内直接调用。
fn grow<'n>(sub: &mut Flow<'n>, node: &'n AddOne, input: Ref<Amount>) -> Ref<Amount> {
    let doubled = sub.then_with_output(double, input);
    sub.then_with_output(node, doubled)
}

/// 最小 executor：把 `Runtime::execute` 的 Future 驱动到完成。
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn main() {
    let output = block_on(async {
        let runtime = Runtime::new();
        let add = AddOne;
        runtime
            .execute::<_, Amount, _>(
                |flow, amount| {
                    // 21 -> double -> 42
                    let doubled = flow.then_with_output(double, amount);
                    // 42 -> (chain via closure factory: double + 1) -> 43
                    let plus_one = flow.chain(fragment(&add, doubled));
                    // 43 -> triple -> 129
                    let tripled = flow.then_with_output(triple, plus_one);
                    // 129 -> (chain via `&mut Flow<'n>` function: double + 1) -> 259
                    let finished = flow.chain(|sub| grow(sub, &add, tripled));
                    flow.output_with(finished);
                },
                Amount(21),
            )
            .await
    })
    .unwrap();
    assert_eq!(output.0, 259, "closure factory + &mut Flow fragment forms");

    println!("we_t02-positive-ok");
}
