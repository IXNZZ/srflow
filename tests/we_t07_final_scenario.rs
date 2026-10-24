//! V22-T07 外部消费者（二）：五结构同次执行的最终端到端场景（**普通控制入口**）。
//!
//! 同一次 `Runtime::execute` 内使用**普通控制方法**（`chain`／`choose`／`each`／`retry`／`iter`，
//! 均非 `_output` 形式）组合 Chain／Choose／Each／Retry／Iter，混合**自动**入口
//! （`then`／`then_output`）与**显式**入口（`then_with_output`）与同步 Factory，最终以父域多槽
//! 输出交付。
//!
//! 判别性证据：
//! - 每个普通控制方法的**声明输出**都由紧随其后的自动 `then` 下游节点读取并**断具体新值**，
//!   再以 `ref_of` 解析同一发布来源进入父域输出；把任一普通登记路径改成空操作（或跳过登记）
//!   都会使下游断值或 `ref_of` 解析失败；
//! - 节点/分支/项/轮次计数逐一断言（含未选分支零执行、Factory 逐 attempt）；
//! - Retry 由专用 attempt 计数驱动：首次发出 Retry 控制请求、第二次成功，断言恰好两次。
//!
//! 只用 crate 根公开面。运行：`cargo test --offline --test we_t07_final_scenario`
#![allow(clippy::unused_unit, clippy::let_unit_value)]

use std::cell::Cell;

use futures::executor::block_on;
use srflow::{BodyError, Data, Query, Ref, Runtime};

// ---------------------------------------------------------------- 业务类型

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Amount(u32);

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Key(u32);

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Item(u32);

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Score(u32);

#[derive(Clone, Debug, PartialEq, Eq, Data)]
struct Merged(u32);

thread_local! {
    static CHAIN_AUTO: Cell<usize> = const { Cell::new(0) };
    static CHAIN_FACTORY: Cell<usize> = const { Cell::new(0) };
    static CHAIN_MERGE: Cell<usize> = const { Cell::new(0) };
    static DOWNSTREAM_CHAIN: Cell<usize> = const { Cell::new(0) };
    static DOWNSTREAM_CHOSEN: Cell<usize> = const { Cell::new(0) };
    static DOWNSTREAM_SCORES: Cell<usize> = const { Cell::new(0) };
    static DOWNSTREAM_KEY: Cell<usize> = const { Cell::new(0) };
    static DOWNSTREAM_STATE: Cell<usize> = const { Cell::new(0) };
    static CASE_HIT: Cell<usize> = const { Cell::new(0) };
    static OTHERWISE_HIT: Cell<usize> = const { Cell::new(0) };
    static ITEM_SCORE: Cell<usize> = const { Cell::new(0) };
    static ITEM_FACTORY: Cell<usize> = const { Cell::new(0) };
    static ATTEMPT_FACTORY: Cell<usize> = const { Cell::new(0) };
    static FLAKY: Cell<usize> = const { Cell::new(0) };
    static ITER_AUTO: Cell<usize> = const { Cell::new(0) };
    static ITER_JUDGE: Cell<usize> = const { Cell::new(0) };
}

fn reset() {
    CHAIN_AUTO.with(|c| c.set(0));
    CHAIN_FACTORY.with(|c| c.set(0));
    CHAIN_MERGE.with(|c| c.set(0));
    DOWNSTREAM_CHAIN.with(|c| c.set(0));
    DOWNSTREAM_CHOSEN.with(|c| c.set(0));
    DOWNSTREAM_SCORES.with(|c| c.set(0));
    DOWNSTREAM_KEY.with(|c| c.set(0));
    DOWNSTREAM_STATE.with(|c| c.set(0));
    CASE_HIT.with(|c| c.set(0));
    OTHERWISE_HIT.with(|c| c.set(0));
    ITEM_SCORE.with(|c| c.set(0));
    ITEM_FACTORY.with(|c| c.set(0));
    ATTEMPT_FACTORY.with(|c| c.set(0));
    FLAKY.with(|c| c.set(0));
    ITER_AUTO.with(|c| c.set(0));
    ITER_JUDGE.with(|c| c.set(0));
}

fn cell(counter: &'static std::thread::LocalKey<Cell<usize>>) -> usize {
    counter.with(Cell::get)
}

// ---------------------------------------------------------------- 业务 Node

/// Chain 内自动 Node（取输出形式，读 Root 的 Amount）。
async fn chain_auto(query: Query<&Amount>) -> Result<Amount, BodyError> {
    CHAIN_AUTO.with(|c| c.set(c.get() + 1));
    Ok(Amount(query.get().0 + 1))
}

/// Chain 内显式两槽：Key × Amount → Merged。
async fn chain_merge(query: Query<(&Key, &Amount)>) -> Result<Merged, BodyError> {
    CHAIN_MERGE.with(|c| c.set(c.get() + 1));
    let (key, amount) = query.get();
    Ok(Merged(key.0 + amount.0))
}

/// 自动下游：读**普通 `chain`** 发布到父域的声明输出，并断言具体新值。
async fn downstream_chain(query: Query<&Merged>) -> Result<(), BodyError> {
    DOWNSTREAM_CHAIN.with(|c| c.set(c.get() + 1));
    assert_eq!(
        query.get(),
        &Merged(17),
        "普通 chain 必须发布其声明输出供自动下游读取"
    );
    Ok(())
}

/// 自动下游：读**普通 `choose`** 发布的共同输出并断值。
async fn downstream_chosen(query: Query<&Merged>) -> Result<(), BodyError> {
    DOWNSTREAM_CHOSEN.with(|c| c.set(c.get() + 1));
    assert_eq!(
        query.get(),
        &Merged(14),
        "普通 choose 必须发布命中分支的共同输出"
    );
    Ok(())
}

/// 自动下游：读**普通 `each`** 发布的聚合 Vec 并逐元素断值。
async fn downstream_scores(query: Query<&Vec<Score>>) -> Result<(), BodyError> {
    DOWNSTREAM_SCORES.with(|c| c.set(c.get() + 1));
    assert_eq!(
        query.get().as_slice(),
        [Score(3), Score(6), Score(9)],
        "普通 each 必须发布逐项聚合结果"
    );
    Ok(())
}

/// 自动下游：读**普通 `retry`** 发布的成功输出并断值。
async fn downstream_key(query: Query<&Key>) -> Result<(), BodyError> {
    DOWNSTREAM_KEY.with(|c| c.set(c.get() + 1));
    assert_eq!(query.get(), &Key(107), "普通 retry 必须发布最终成功输出");
    Ok(())
}

/// 自动下游：读**普通 `iter`** 发布的最终 State 并断值。
async fn downstream_state(query: Query<&u32>) -> Result<(), BodyError> {
    DOWNSTREAM_STATE.with(|c| c.set(c.get() + 1));
    assert_eq!(*query.get(), 2, "普通 iter 必须发布最终 State");
    Ok(())
}

/// Choose 命中分支（显式 selector 值）。
async fn case_key(query: Query<&Key>) -> Result<Merged, BodyError> {
    CASE_HIT.with(|c| c.set(c.get() + 1));
    Ok(Merged(query.get().0 * 2))
}

/// Choose 未命中分支（必须零执行）。
async fn otherwise_key(query: Query<&Key>) -> Result<Merged, BodyError> {
    OTHERWISE_HIT.with(|c| c.set(c.get() + 1));
    Ok(Merged(query.get().0))
}

/// Each item Body 的显式 Node。
async fn item_score(query: Query<&Item>) -> Result<Score, BodyError> {
    ITEM_SCORE.with(|c| c.set(c.get() + 1));
    Ok(Score(query.get().0 * 3))
}

/// Retry attempt：**首次**发出 Retry 控制请求，第二次成功。
async fn flaky_key(query: Query<&Key>) -> Result<Key, BodyError> {
    let attempt = FLAKY.with(|c| {
        c.set(c.get() + 1);
        c.get()
    });
    if attempt == 1 {
        Err(BodyError::retry(std::io::Error::other("first attempt")))
    } else {
        Ok(Key(query.get().0 + 100))
    }
}

/// Iter Round 内自动 Node。
async fn iter_auto(query: Query<&u32>) -> Result<u32, BodyError> {
    ITER_AUTO.with(|c| c.set(c.get() + 1));
    Ok(query.get() + 1)
}

/// Iter 判据：状态达到 2 时 `IterBreak`。
async fn iter_judge(query: Query<&u32>) -> Result<(), BodyError> {
    ITER_JUDGE.with(|c| c.set(c.get() + 1));
    if *query.get() >= 2 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

/// Iter Round 的唯一 `next` 来源。
async fn iter_advance(query: Query<&u32>) -> Result<u32, BodyError> {
    Ok(query.get() + 1)
}

// ---------------------------------------------------------------- 端到端场景

#[test]
fn t07_five_structures_via_ordinary_entries_compose_in_one_execution() {
    reset();

    let (chained, picked, scores, retried, state) = block_on(Runtime::new().execute::<(
        Amount,
        Key,
        Vec<Item>,
        u32,
    ), (
        Merged,
        Merged,
        Vec<Score>,
        Key,
        u32,
    ), _>(
        |flow, (_amount, key, items, seed)| {
            // ① 普通 `chain`：内部含自动 Node 与同步 Factory；声明输出由自动下游断值。
            flow.chain(|sub| {
                let base: Ref<Amount> = sub.then_output(chain_auto);
                let made: Ref<Key> = sub.provide_output(|| {
                    CHAIN_FACTORY.with(|c| c.set(c.get() + 1));
                    Key(7)
                });
                let merged = sub.then_with_output(chain_merge, (made, base));
                sub.output_with(merged);
            });
            let chained: Ref<Merged> = flow.ref_of::<Merged>();
            let _ = flow.then(downstream_chain);

            // ② 普通 `choose`（自动 selector）：命中分支唯一运行，共同输出由自动下游断值。
            flow.choose(|choice| {
                choice.case(Key(7), |branch| {
                    let value = branch.then_with_output(case_key, key);
                    branch.output_with(value);
                });
                choice.otherwise(|branch| {
                    let value = branch.then_with_output(otherwise_key, key);
                    branch.output_with(value);
                });
            });
            let picked: Ref<Merged> = flow.ref_of::<Merged>();
            let _ = flow.then(downstream_chosen);

            // ③ 普通 `each`：item Body 内混合自动与显式 Node 及 Factory；聚合结果由自动下游断值。
            flow.each(items, |sub| {
                let item = sub.ref_of::<Item>();
                let driver: Ref<Score> = sub.then_output(item_score_driver);
                let made: Ref<Score> = sub.provide_output(|| {
                    ITEM_FACTORY.with(|c| c.set(c.get() + 1));
                    Score(5)
                });
                let scored = sub.then_with_output(item_score, item);
                let _ = (driver, made);
                sub.output_with(scored);
            });
            let scores: Ref<Vec<Score>> = flow.ref_of::<Vec<Score>>();
            let _ = flow.then(downstream_scores);

            // ④ 普通 `retry`：第一次 attempt 发 Retry，第二次成功；成功输出由自动下游断值。
            flow.retry(1, |attempt| {
                let _: Ref<Score> = attempt.provide_output(|| {
                    ATTEMPT_FACTORY.with(|c| c.set(c.get() + 1));
                    Score(1)
                });
                let value = attempt.then_with_output(flaky_key, key);
                attempt.output_with(value);
            });
            let retried: Ref<Key> = flow.ref_of::<Key>();
            let _ = flow.then(downstream_key);

            // ⑤ 普通 `iter`：Round 内自动 Node + 判据 + 恰好一次 `next`；最终 State 由自动下游断值。
            flow.iter(seed, 5, |round| {
                let _ = round.then(iter_auto);
                let current = round.current::<u32>();
                let _: () = round.then_with_output(iter_judge, current);
                let next = round.then_with_output(iter_advance, current);
                round.next_with(next);
            });
            let state: Ref<u32> = flow.ref_of::<u32>();
            let _ = flow.then(downstream_state);

            flow.output_with((chained, picked, scores, retried, state));
        },
        (Amount(9), Key(7), vec![Item(1), Item(2), Item(3)], 0u32),
    ))
    .expect("五结构经普通控制入口同次执行必须成功");

    // 父域最终输出逐槽（全部经 `ref_of` 解析普通控制方法的**发布**来源）。
    assert_eq!(
        chained,
        Merged(17),
        "普通 chain 发布值：Key(7) + Amount(10)"
    );
    assert_eq!(
        picked,
        Merged(14),
        "普通 choose 发布值：命中分支 Key(7) × 2"
    );
    assert_eq!(
        scores,
        vec![Score(3), Score(6), Score(9)],
        "普通 each 发布逐项聚合"
    );
    assert_eq!(retried, Key(107), "普通 retry 发布第二次 attempt 的成功值");
    assert_eq!(state, 2, "普通 iter 发布第三轮 IterBreak 交付的 Current");

    // 逐项计数：生产路径与自动下游分开断言。
    assert_eq!(cell(&CHAIN_AUTO), 1, "Chain 内自动 Node 一次");
    assert_eq!(cell(&CHAIN_FACTORY), 1, "Chain 内 Factory 一次");
    assert_eq!(cell(&CHAIN_MERGE), 1, "Chain 内显式两槽一次");
    assert_eq!(
        cell(&DOWNSTREAM_CHAIN),
        1,
        "普通 chain 的自动下游一次（断值 Merged(17)）"
    );
    assert_eq!(cell(&CASE_HIT), 1, "Choose 命中分支一次");
    assert_eq!(cell(&OTHERWISE_HIT), 0, "未选分支零执行");
    assert_eq!(
        cell(&DOWNSTREAM_CHOSEN),
        1,
        "普通 choose 的自动下游一次（断值 Merged(14)）"
    );
    assert_eq!(cell(&ITEM_SCORE), 3, "Each 三项各一次");
    assert_eq!(cell(&ITEM_FACTORY), 3, "Each 三项各一次 Factory");
    assert_eq!(
        cell(&DOWNSTREAM_SCORES),
        1,
        "普通 each 的自动下游一次（逐元素断值）"
    );
    assert_eq!(cell(&ATTEMPT_FACTORY), 2, "两次 attempt 各一次 Factory");
    assert_eq!(
        cell(&FLAKY),
        2,
        "Retry 恰好两次 attempt（首次 Retry、第二次成功）"
    );
    assert_eq!(
        cell(&DOWNSTREAM_KEY),
        1,
        "普通 retry 的自动下游一次（断值 Key(107)）"
    );
    assert_eq!(cell(&ITER_AUTO), 3, "Iter 三轮各一次自动 Node");
    assert_eq!(cell(&ITER_JUDGE), 3, "Iter 三轮各一次判据");
    assert_eq!(
        cell(&DOWNSTREAM_STATE),
        1,
        "普通 iter 的自动下游一次（断值 2）"
    );
}

/// Each item Body 内的自动驱动 Node（普通形式，发布但不取句柄）。
async fn item_score_driver(query: Query<&Item>) -> Result<Score, BodyError> {
    Ok(Score(query.get().0))
}
