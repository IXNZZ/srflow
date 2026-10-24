//! T05 迁移自 `v22_t03_from_we_t03_c05_wrong_key_type.rs`（承接 WE-T03 C05 负例）。
//!
//! V22-T05 承接 WE-T03 C05 负例：selector 与 case 键类型不一致必须在编译期拒绝。
//!
//! `choose_with(selector: Ref<K>, …)` 固定键类型 `K`。本夹具的 selector 是 `Ref<Amount>`，
//! 而 `case` 传的是 `Route`；`Amount` 已实现 `PartialEq`，因此这里唯一的目标诊断是
//! 键类型不匹配，而不是缺少 `PartialEq`（对比 `v22_t05_from_we_t03_c05_key_without_partialeq.rs`）。
//!
//! T05 契约迁移：旧 `flow.choose(selector, …)`（闭包返回 RefShape）改为
//! `flow.choose_with_output::<K, O, _>(selector, …)`；分支改为 unit Body，在内部用
//! `branch.output_with(...)` 声明输出。
//!
//! 期望主诊断：E0308（`case` 的键实参类型与 `K` 不一致）。
use srflow::{BodyError, Data, Query, Ref, Runtime};

/// 夹具自带的极小 executor：保持与正例相同的装配形态；本项在编译期被拒绝。
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

#[derive(Data, PartialEq)]
struct Amount(u32);

#[derive(Data, PartialEq)]
struct Route(u32);

async fn id_amount(query: Query<&Amount>) -> Result<Amount, BodyError> {
    Ok(Amount(query.get().0))
}

fn main() {
    let runtime = Runtime::new();
    let _ = drive(runtime.execute::<_, Amount, _>(
        |flow, amount: Ref<Amount>| {
            let picked = flow.choose_with_output::<Amount, Ref<Amount>, _>(amount, |choice| {
                // 期望主诊断：E0308——K 已由 selector 定为 Amount，此处键是 Route。
                choice.case(Route(0), |branch| { // 键应为 Amount
                    let out = branch.then_with_output(id_amount, amount);
                    branch.output_with(out);
                });
                choice.otherwise(|branch| {
                    let out = branch.then_with_output(id_amount, amount);
                    branch.output_with(out);
                });
            });
            flow.output_with(picked);
        },
        Amount(1),
    ));
}
