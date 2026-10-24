//! 日常独立 Cargo 消费者：**重命名依赖 + 派生宏 + 正式公开面** 的外部端到端样本。
//!
//! 本工程把主包 `srflow` 以依赖别名 `roundkit` 引入并**自成 workspace 根**（空 `[workspace]` 表），
//! 走真实 Cargo 消费者路径：`#[derive(Data)]` 经 `proc-macro-crate` 读取本工程自己的 Cargo.toml，
//! 必须解析出实际依赖名 `roundkit` 才能展开；raw `rustc --extern <name>=<rlib>` 无法表达该环节。
//!
//! 覆盖（长期场景 K13 的外部使用者验证）：
//! 1. 单 Root：单业务值输入 + 借用 Struct Node（owned 形式）真实执行；
//! 2. **五结构同次执行的普通入口**：`chain`／`choose`／`each`／`retry`／`iter` 均用非 `_output`
//!    入口登记，各自声明的产物由后继 `ref_of` 读到并断新值；Retry 由 attempt 计数证明确实重试；
//! 3. 16 槽 Root 输入：上限边界的连接与整组 owned 提取；
//! 4. 断言失败即非零退出，成功打印 `test-cleanup-consumer-ok`。
//!
//! 运行：`cargo run --offline --quiet --locked --manifest-path tests/support/consumer/Cargo.toml`
use futures::executor::block_on;
use roundkit::{BodyError, Data, Flow, Node, Query, Ref, Runtime};
use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

// ================================================================ 业务 Data

#[derive(Data)]
struct Seed(u32);

#[derive(Data)]
struct Amount(u32);

#[derive(Data)]
struct Report(u32);

#[derive(Data)]
struct Item(u32);

#[derive(Data)]
struct Mark(u32);

#[derive(Data)]
struct Draft(String);

/// Choose 键：只声明 `Data` 并手写 `PartialEq`（最小约束，不派生任何额外 trait）。
#[derive(Data)]
struct Route(u32);

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

/// Retry 的动态失败原因。
#[derive(Debug)]
struct NotReady {
    value: u32,
}

impl fmt::Display for NotReady {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "not ready: {}", self.value)
    }
}

impl Error for NotReady {}

static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static MARKED: AtomicUsize = AtomicUsize::new(0);
static FALLBACKS: AtomicUsize = AtomicUsize::new(0);

// ================================================================ 业务 Node

async fn amount_of(query: Query<&Seed>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

async fn double(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

async fn plus_one(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 + 1))
}

/// 第一次 attempt 动态失败，第二次成功——重试是真实发生的，不是空跑。
async fn judge_retry_once(query: Query<&Amount>) -> Result<Report, BodyError> {
    let attempt = ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
    let value = query.get().0;
    if attempt == 1 {
        Err(BodyError::retry(NotReady { value }))
    } else {
        Ok(Report(value * 10))
    }
}

async fn route_of(query: Query<&Report>) -> Result<Route, BodyError> {
    Ok(Route(query.get().0 % 2))
}

/// 命中分支的产物：与 Iter 状态值刻意不同（同名值无法区分来源）。
async fn double_on_match(query: Query<&Report>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0 * 2))
}

/// 未命中分支：被调用即计数，用于证明未选分支零执行。
async fn offset_on_fallback(query: Query<&Report>) -> Result<Amount, BodyError> {
    FALLBACKS.fetch_add(1, Ordering::SeqCst);
    Ok(Amount(query.get().0 + 2))
}

async fn items_of(query: Query<&Seed>) -> Result<Vec<Item>, BodyError> {
    let value = query.get().0;
    Ok(vec![Item(value), Item(value + 1)])
}

async fn mark_of(query: Query<&Item>) -> Result<Mark, BodyError> {
    MARKED.fetch_add(1, Ordering::SeqCst);
    Ok(Mark(query.get().0 * 10))
}

async fn draft_of(query: Query<&Seed>) -> Result<Draft, BodyError> {
    Ok(Draft(format!("d{}", query.get().0)))
}

async fn revise(query: Query<&Draft>) -> Result<Draft, BodyError> {
    Ok(Draft(format!("{}+", query.get().0)))
}

async fn bump_report(query: Query<&Report>) -> Result<Report, BodyError> {
    Ok(Report(query.get().0 + 1))
}

/// Iter round 终止判断：使 Round 在推进后发出 `IterBreak`（交付入轮 current）。
async fn judge_break(query: Query<&Draft>) -> Result<(), BodyError> {
    if query.get().0.contains('+') {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

/// owned Struct Node：借用形式注册，用于验证 `Node` trait 的具名实现路径。
struct Tripler {
    factor: u32,
}

impl Node for Tripler {
    type Input = Amount;
    type Output = Amount;

    async fn run(&self, query: Query<&Self::Input>) -> Result<Amount, BodyError> {
        Ok(Amount(query.get().0 * self.factor))
    }
}

// ================================================================ 主流程

fn main() {
    let runtime = Runtime::new();

    // 1) 单 Root + owned Struct Node。
    let tripler = Tripler { factor: 3 };
    let single: Amount = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<Seed>| {
            let amount = flow.then_with_output(amount_of, seed);
            let tripled = flow.then_with_output(&tripler, amount);
            flow.output_with(tripled);
        },
        Seed(7),
    ))
    .expect("single-input root runs under the renamed dependency");
    assert_eq!(single.0, 21, "7 * 3");

    // 2) 五结构同次执行，全部走普通入口；产物由后继 `ref_of` 读取并断新值。
    ATTEMPTS.store(0, Ordering::SeqCst);
    MARKED.store(0, Ordering::SeqCst);
    FALLBACKS.store(0, Ordering::SeqCst);
    let (picked, marks, draft, context): (Amount, Vec<Mark>, Draft, Report) = block_on(
        runtime.execute(
            |flow: &mut Flow<'_>, seed: Ref<Seed>| {
                let amount = flow.then_with_output(amount_of, seed);
                // 普通 chain：登记产物，父域随后用 ref_of 读取。
                flow.chain(|sub| {
                    let doubled = sub.then_with_output(double, amount);
                    let grown = sub.then_with_output(plus_one, doubled);
                    sub.output_with(grown);
                });
                let grown: Ref<Amount> = flow.ref_of();
                // 普通 retry：attempt 内首次失败、第二次成功。
                flow.retry(2, |attempt| {
                    let candidate = attempt.then_with_output(plus_one, grown);
                    let judged = attempt.then_with_output(judge_retry_once, candidate);
                    attempt.output_with(judged);
                });
                let report: Ref<Report> = flow.ref_of();
                // 普通 choose：命中分支与回退分支各声明一个产物。
                let selector = flow.then_with_output(route_of, report);
                flow.choose_with(selector, |choice| {
                    choice.case(Route(0), |branch| {
                        let value = branch.then_with_output(double_on_match, report);
                        branch.output_with(value);
                    });
                    choice.otherwise(|branch| {
                        let value = branch.then_with_output(offset_on_fallback, report);
                        branch.output_with(value);
                    });
                });
                let picked: Ref<Amount> = flow.ref_of();
                // 普通 each：逐项创建 item Body，产物聚合成 Vec。
                let items = flow.then_with_output(items_of, seed);
                flow.each(items, |sub| {
                    let item = sub.ref_of::<Item>();
                    let mark = sub.then_with_output(mark_of, item);
                    sub.output_with(mark);
                });
                let marks: Ref<Vec<Mark>> = flow.ref_of();
                // 普通 iter：多轮推进，Break 交付入轮 current，同时发布终结 State。
                let initial = flow.then_with_output(draft_of, seed);
                flow.iter((initial, report), 4, |round| {
                    let (current_draft, current_context) = round.current::<(Draft, Report)>();
                    round.then_with_output(judge_break, current_draft);
                    let next_draft = round.then_with_output(revise, current_draft);
                    let next_context = round.then_with_output(bump_report, current_context);
                    round.next_with((next_draft, next_context));
                });
                let draft: Ref<Draft> = flow.ref_of();
                let context: Ref<Report> = flow.ref_of();
                flow.output_with((picked, marks, draft, context));
            },
            Seed(3),
        ),
    )
    .expect("five structures under ordinary entries run in one execution");
    assert_eq!(picked.0, 160, "retry 后 Report(80) → 命中分支乘二；回退分支的 +2 会给出 82");
    assert_eq!(
        marks.iter().map(|mark| mark.0).collect::<Vec<_>>(),
        vec![30, 40],
        "逐项产出按 item 顺序聚合"
    );
    assert_eq!(draft.0, "d3+", "Break 交付的是入轮 current");
    assert_eq!(
        context.0, 81,
        "iter 普通入口发布的终结 State（Break 轮的入轮 Report 80 → +1）"
    );
    assert_eq!(FALLBACKS.load(Ordering::SeqCst), 0, "未选分支零执行");
    assert_eq!(ATTEMPTS.load(Ordering::SeqCst), 2, "确实发生一次真实重试");
    assert_eq!(MARKED.load(Ordering::SeqCst), 2, "Each 对每项各调用一次 item Body");

    // 3) 16 槽 Root 输入：上限边界的连接与整组 owned 提取。
    type Sixteen = (
        u8,
        u16,
        u32,
        u64,
        u128,
        usize,
        i8,
        i16,
        i32,
        i64,
        i128,
        isize,
        bool,
        char,
        f32,
        f64,
    );
    let input16: Sixteen = (
        1u8, 2u16, 3u32, 4u64, 5u128, 6usize, 7i8, 8i16, 9i32, 10i64, 11i128, 12isize, true,
        'x', 1.5f32, 2.5f64,
    );
    let output16: Sixteen = block_on(runtime.execute::<_, Sixteen, _>(
        |flow, refs| {
            let _: () = flow.then_with_output(consume_sixteen, refs);
            flow.output_with(refs);
        },
        input16,
    ))
    .expect("16-position root runs under the renamed dependency");
    assert_eq!(output16.0, 1u8, "首位置往返");
    assert_eq!(output16.15, 2.5f64, "末位置往返");

    println!("test-cleanup-consumer-ok");
}

/// 16 槽 Node：只读代表位置（首/末），证明输入确实到达业务代码。
async fn consume_sixteen(
    query: Query<(
        &u8,
        &u16,
        &u32,
        &u64,
        &u128,
        &usize,
        &i8,
        &i16,
        &i32,
        &i64,
        &i128,
        &isize,
        &bool,
        &char,
        &f32,
        &f64,
    )>,
) -> Result<(), BodyError> {
    let slots = query.get();
    assert_eq!(*slots.0, 1u8, "16 槽 Node 读到首位置");
    assert_eq!(*slots.15, 2.5f64, "16 槽 Node 读到末位置");
    Ok(())
}
