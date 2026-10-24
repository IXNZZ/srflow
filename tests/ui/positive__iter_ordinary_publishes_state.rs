//! T06 新增正例样本 ⑨：普通 `iter` 的最终 State 照常作为父域来源被后继 Node 真实读取。
//!
//! 普通形式返回 `&mut Flow`（不返回 Ref），但它在 parent 域发布最终 State 位置：后继
//! `ref_of::<State>()` + `then_with_output` 必须读到 Break 交付的值（这里第一轮就 Break，值不变）。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we-t06-ordinary-iter-publishes-ok`。
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};

#[derive(Data)]
struct State(u32);

#[derive(Data)]
struct Score(u32);

static CALLS: AtomicUsize = AtomicUsize::new(0);

async fn bump(query: Query<&State>) -> Result<State, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(State(query.get().0 + 1))
}

async fn break_now(_: Query<&State>) -> Result<(), BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Err(BodyError::iter_break())
}

async fn score_of(query: Query<&State>) -> Result<Score, BodyError> {
    CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(Score(query.get().0 * 10))
}

fn main() {
    let runtime = Runtime::new();
    CALLS.store(0, Ordering::SeqCst);
    let report = block_on(runtime.execute::<_, Score, _>(
        |flow: &mut Flow<'_>, initial: Ref<State>| {
            flow.iter(initial, 2, |round| {
                let current = round.current::<State>();
                round.then_with_output(&bump, current);
                round.then_with_output(&break_now, current);
                round.next_with(current);
            });
            // 普通形式不返回 Ref；最终 State 必须已被发布为父域来源。
            let published = flow.ref_of::<State>();
            let report = flow.then_with_output(&score_of, published);
            flow.output_with(report);
        },
        State(1),
    ))
    .expect("普通 iter 的最终 State 可被后继 Node 读取");
    assert_eq!(report.0, 10, "后继 Node 读到的是 Break 交付的 1");
    assert!(
        CALLS.load(Ordering::SeqCst) >= 3,
        "Round 内两个 Node 与后继 Node 都真实执行"
    );
    println!("we_t06 ordinary iter published the final state");
    println!("we-t06-ordinary-iter-publishes-ok");
}
