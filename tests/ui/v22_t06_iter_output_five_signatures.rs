//! T06 新增正例样本 ⑧：五签名端到端真实执行（`iter_output` + `current` + `next_with`）。
//!
//! 两槽同类型状态 `(State, State)`：Round Body 用 `current::<(State, State)>()` 取得逐槽 Current，
//! 第二轮 Break 前把 Next 声明为 `(y, x)`（跨槽交换）；交付值必须是交换后的 `(2, 1)`。
//!
//! 期望：编译成功，运行退出码 0，stdout 含 `we-t06-five-signatures-ok`。

use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use srflow::{BodyError, Data, Flow, Query, Ref, Runtime};

#[derive(Data)]
struct State(u32);

static ROUNDS: AtomicUsize = AtomicUsize::new(0);

/// 第二轮才 Break：第一轮的 Normal Promote 必须先被接受。
async fn break_on_second(_: Query<&State>) -> Result<(), BodyError> {
    let round = ROUNDS.fetch_add(1, Ordering::SeqCst) + 1;
    if round >= 2 {
        return Err(BodyError::iter_break());
    }
    Ok(())
}

fn main() {
    let runtime = Runtime::new();
    ROUNDS.store(0, Ordering::SeqCst);
    let (first, second) = block_on(runtime.execute::<_, (State, State), _>(
        |flow: &mut Flow<'_>, (a, b): (Ref<State>, Ref<State>)| {
            let out_refs = flow.iter_output((a, b), 5, |round| {
                let (x, y) = round.current::<(State, State)>();
                round.then_with_output(&break_on_second, x);
                round.next_with((y, x));
            });
            flow.output_with(out_refs);
        },
        (State(1), State(2)),
    ))
    .expect("第二轮 Break 交付交换后的状态");
    assert_eq!(
        (first.0, second.0),
        (2, 1),
        "跨槽交换必须由 current + next_with 真实表达"
    );
    println!("we_t06 five signatures executed with swapped slots");
    println!("we-t06-five-signatures-ok");
}
