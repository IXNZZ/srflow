//! V22-T03 夹具迁移（源自 WE-T06 `we_t06_p03_chain_fragment.rs`）：Chain 与 Fragment（SPEC §7）。
//!
//! 原目的（X24/S03／C13／C15）：Chain 建立真实 child 边界（只保留声明输出、清理 child-owned
//! 中间数据）、两层 chain 的祖先捕获（Import）、Fragment 的两种写法（closure factory 与接收
//! `&mut Flow` 的普通函数），以及 child-local Ref 不泄漏到 parent；断言 `output = 22` 且
//! child-owned 中间值 `Drop` 恰一次。
//!
//! 迁移手法：旧两参 `flow.then(node, refs)`（含 fragment 内部的 `sub.then(node, input)`）改为
//! `then_with_output`；只有 Root 闭包改为固定返回 `()` 并经 `output_with` 声明 owned `Out`
//! （`execute::<_, Amount, _>`）。chain／child 契约与断言不变。

use std::cell::RefCell;
use std::rc::Rc;

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Node, Query, Ref, Runtime};

#[derive(Data)]
struct Amount(u32);

/// Root 输入：业务值 + 事件记录句柄（随数据流传播，证明祖先捕获真实可用）。
#[derive(Data)]
struct Seed {
    value: u32,
    journal: Rc<RefCell<Vec<&'static str>>>,
}

/// child-owned 中间产物：Drop 记录一次真实析构（证明 child 边界负责清理）。
struct Temp {
    journal: Rc<RefCell<Vec<&'static str>>>,
}

impl Data for Temp {}

impl Drop for Temp {
    fn drop(&mut self) {
        self.journal.borrow_mut().push("drop:temp");
    }
}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn plus_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

/// 由祖先输入产生一个**非声明输出**的 child-owned 中间值。
async fn temp_of(query: Query<&Seed>) -> Result<Temp, BodyError> {
    Ok(Temp {
        journal: Rc::clone(&query.get().journal),
    })
}

async fn seed_amount(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value))
}

struct Doubler;

impl Node for Doubler {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 * 2))
    }
}

/// Fragment 写法一：closure factory，携带 Node 句柄的借用（非 static 依赖）。
fn step_fragment<'n>(
    node: &'n Doubler,
    input: Ref<Amount>,
) -> impl FnOnce(&mut Flow<'n>) -> Ref<Amount> + 'n {
    move |sub| sub.then_with_output(node, input)
}

/// Fragment 写法二：接收 `&mut Flow` 的普通函数。
fn step_inline<'n>(sub: &mut Flow<'n>, input: Ref<Amount>) -> Ref<Amount> {
    sub.then_with_output(plus_one, input)
}

fn main() {
    let journal = Rc::new(RefCell::new(Vec::new()));
    let doubler = Doubler;
    let runtime = Runtime::new();

    let output = block_on(runtime.execute::<_, Amount, _>(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            // 外层 chain：祖先捕获 seed（真实 Import），child 内另有 child-owned 中间值。
            let checked = flow.chain(|sub| {
                let _temp: Ref<Temp> = sub.then_with_output(temp_of, seed);
                let doubled = sub.then_with_output(seed_amount, seed);
                // 内层 chain：Fragment 两种写法各一次（闭包 factory＋普通函数），逐层接线。
                let via_fragment = sub.chain(step_fragment(&doubler, doubled));
                sub.chain(|inner| step_inline(inner, via_fragment))
            });
            // parent 继续读取 child 的**声明输出**（不是 child-local 位置）。
            let out_refs = flow.chain(|sub| sub.then_with_output(double, checked));
            flow.output_with(out_refs);
        },
        Seed {
            value: 5,
            journal: Rc::clone(&journal),
        },
    ))
    .expect("chain and fragments run");

    assert_eq!(output.0, 22, "5 -> 10 -> 11 -> 22");
    assert_eq!(
        journal.borrow().as_slice(),
        ["drop:temp"],
        "child-owned 中间值的析构由 child 边界负责，恰好一次"
    );

    println!("we_t06 p03 output={}", output.0);
    println!("we_t06-positive-ok");
}
