//! V22-T03 承接 WE-T03 C02 正例：Choose 的**最小键约束**与借用／非 static／非 Send 边界。
//!
//! 覆盖（SPEC §9；任务书 C02）：
//!
//! - 匹配键 `K` 只要求 `Data + PartialEq`：本夹具的 `Tag` 通过 `#[derive(Data)]` 声明，
//!   并**手写** `PartialEq`；它不实现 `Eq` / `Hash` / `Ord` / `Clone` / `Copy` / `Debug`。
//! - 键含 `Rc<str>` 字段（整体非 `Send`）；键按值传入 `case`，由普通函数每次新建，
//!   不依赖 `Clone`。
//! - 业务输入值 `Seed` 含 `Rc<Cell<u32>>`，整体非 `Send`。
//! - `Configured<'a>` 借用仅在 `main` 局部存活的 `u32`（非 `'static` Node 配置），
//!   在分支定义内以 `&configured` 登记。
//!
//! T03 契约迁移：旧两参 `flow.then(node, refs)` 改为 `flow.then_with_output(node, refs)`；
//! Root 闭包固定返回 `()`，业务输出由 `Flow::output_with` 声明（owned `Out = Amount`
//! 由局部类型标注给出）。
//!
//! 以真实主包 rlib 外部编译并真实运行；期望退出码 0，stdout 含
//! `t03-c02-minimal-key-and-borrowed-node-ok`。

use std::cell::Cell;
use std::rc::Rc;

use srflow::{BodyError, Data, Node, Query, Runtime};

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

/// 业务输入值：含 `Rc<Cell<u32>>`，整体非 `Send`（本版不要求 Root Future 可跨线程）。
#[derive(Data)]
struct Seed {
    value: Rc<Cell<u32>>,
}

#[derive(Data)]
struct Amount(u32);

/// 匹配键：`#[derive(Data)]` + 手写 `PartialEq`；含 `Rc` 字段；**不**实现
/// `Eq` / `Hash` / `Ord` / `Clone` / `Copy` / `Debug`。
#[derive(Data)]
struct Tag {
    name: Rc<str>,
    level: u32,
}

impl PartialEq for Tag {
    fn eq(&self, other: &Self) -> bool {
        self.level == other.level && *self.name == *other.name
    }
}

/// 每次新建一个键值；`Tag` 既非 `Copy` 也非 `Clone`，只能按值移动进 `case`。
fn tag(name: &str, level: u32) -> Tag {
    Tag {
        name: Rc::from(name),
        level,
    }
}

/// 借用非 static 局部配置的 Node（配置类型 `Configured<'a>` 本身不是 `'static`）。
struct Configured<'a> {
    delta: &'a u32,
}

impl Node for Configured<'_> {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Amount>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + *self.delta))
    }
}

struct Stepper;

impl Node for Stepper {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Amount>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 + 1))
    }
}

async fn read_seed(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().value.get()))
}

async fn classify(query: Query<&Amount>) -> Result<Tag, BodyError> {
    let amount = query.get().0;
    Ok(Tag {
        name: Rc::from(if amount % 2 == 0 { "even" } else { "odd" }),
        level: amount % 2,
    })
}

fn main() {
    let delta = 5u32;
    let configured = Configured { delta: &delta };
    let runtime = Runtime::new();

    let output: Amount = drive(runtime.execute(
        |flow, seed| {
            let amount = flow.then_with_output(read_seed, seed);
            let selector = flow.then_with_output(classify, amount);
            let picked = flow.choose(selector, |choice| {
                choice.case(tag("even", 0), |branch| {
                    branch.then_with_output(&configured, amount)
                });
                choice.otherwise(|branch| branch.then_with_output(Stepper, amount));
            });
            flow.output_with(picked);
        },
        Seed {
            value: Rc::new(Cell::new(4)),
        },
    ))
    .expect("minimal-key choose run");

    // 命中 case：4 + delta(5) = 9；借用非 static Node 在选中分支内真实执行。
    assert_eq!(output.0, 9, "Rc key matched; borrowed non-static Node ran");
    println!("t03-c02-minimal-key-and-borrowed-node-ok");
}
