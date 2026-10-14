//! WE-T04 定向样本（arity批）：Retry 输出与 Iter 状态的 arity 覆盖。
//!
//! 全部用例只通过公开入口 `Runtime::new().execute(...)` 运行。Retry 覆盖 unit、单 Ref
//! 与 2～16 全部 flat tuple arity（宏逐槽断言）；Iter 覆盖单 Ref 与 2～16 全部 arity，
//! 每例至少一轮 `Normal` 后 `IterBreak`，逐槽断言最终交付值（证明 formal current 经
//! StateImport 推进而非重绑 parent Ref）。另含 `Box<(A, B)>` 作为单个 Data 叶子的正例。
//!
//! 17 槽／nested tuple 是纯类型级拒绝（`RefShape`／`StateShape` 只实现到 16 个 flat
//! tuple；见 `shape.rs` 的 `tuple_shapes!`／`tuple_state_shapes!`），在正例 `#[test]`
//! 中无法表达，属编译失败夹具（UI fixture）职责。

use crate::Data;
use crate::core::engine::error::BodyError;
use crate::core::engine::flow::Flow;
use crate::core::engine::runtime::Runtime;
use crate::core::engine::shape::{Query, Ref};
use futures::executor::block_on;

// ---------------------------------------------------------------- arity 槽类型

/// arity 样本的槽类型：提供确定初值与“推进一格”的变换，使每个槽有可断言的具体值。
trait AritySlot: Data {
    /// 该类型槽的确定初值（各类型互不相同，逐槽可区分）。
    const BOOT: Self;
    /// 推进一格；用于 attempt／round 内产生与初值不同的下一值。
    fn bumped(&self) -> Self;
}

macro_rules! arity_slot {
    ($($t:ty = $boot:expr, |$slot:ident| $body:expr;)+) => {$(
        impl AritySlot for $t {
            const BOOT: Self = $boot;
            fn bumped(&self) -> Self {
                let $slot = self;
                $body
            }
        }
    )+};
}

arity_slot!(
    u8 = 1, |s| *s + 1;
    u16 = 2, |s| *s + 1;
    u32 = 3, |s| *s + 1;
    u64 = 4, |s| *s + 1;
    u128 = 5, |s| *s + 1;
    usize = 6, |s| *s + 1;
    i8 = 7, |s| *s + 1;
    i16 = 8, |s| *s + 1;
    i32 = 9, |s| *s + 1;
    i64 = 10, |s| *s + 1;
    i128 = 11, |s| *s + 1;
    isize = 12, |s| *s + 1;
    bool = false, |s| !*s;
    char = 'a', |s| char::from_u32(*s as u32 + 1).unwrap();
    f32 = 13.0, |s| *s + 1.0;
    f64 = 14.0, |s| *s + 1.0;
);

/// 单输出 Node：读入槽值并推进一格（attempt／round 内的自产值）。
async fn bump<T: AritySlot>(query: Query<&T>) -> Result<T, BodyError> {
    Ok(query.get().bumped())
}

/// 评审 Node：状态首槽达到 1 时发出 `IterBreak`（保证至少一轮 `Normal`）。
async fn judge_break_after_first_round(query: Query<&u32>) -> Result<(), BodyError> {
    if *query.get() >= 1 {
        Err(BodyError::iter_break())
    } else {
        Ok(())
    }
}

/// unit 输出 Node：仅消费输入，不产生业务槽（Retry unit 样本使用）。
async fn consume_u32(query: Query<&u32>) -> Result<(), BodyError> {
    let _ = query.get();
    Ok(())
}

// ---------------------------------------------------------------- Retry arity

/// 生成一个 Retry flat-tuple arity 样本：attempt 内逐槽产生新值，成功交付后逐槽断言。
macro_rules! retry_arity_case {
    ($name:ident, $arity:literal, $( $slot:ident : $t:ty ),+ $(,)?) => {
        #[test]
        fn $name() {
            let runtime = Runtime::new();
            let outcome = block_on(runtime.execute(
                |flow: &mut Flow<'_>, refs| {
                    let ($($slot,)+) = refs;
                    flow.retry(1, |attempt| ($(attempt.then(bump::<$t>, $slot),)+))
                },
                ($(<$t as AritySlot>::BOOT,)+),
            ));
            let ($($slot,)+) = outcome.expect("retry delivers every slot");
            $(assert_eq!(
                $slot,
                <$t as AritySlot>::BOOT.bumped(),
                concat!("arity ", $arity, " slot ", stringify!($slot))
            );)+
        }
    };
}

// E02／E18：Retry unit 输出——attempt 正常结束即成功交付，无业务槽。
#[test]
fn retry_unit_output_delivers_success_without_slots() {
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow: &mut Flow<'_>, value: Ref<u32>| {
            flow.retry(1, |attempt| {
                let _: () = attempt.then(&consume_u32, value);
            })
        },
        7u32,
    ));
    assert_eq!(outcome.expect("unit retry delivers"), ());
}

// E18：Retry 单 Ref 输出——逐值断言（41 -> 42）。
#[test]
fn retry_single_ref_output_delivers_the_bumped_value() {
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow: &mut Flow<'_>, value: Ref<u32>| {
            flow.retry(1, |attempt| attempt.then(bump::<u32>, value))
        },
        41u32,
    ));
    assert_eq!(outcome.expect("single-ref retry delivers"), 42u32);
}

// E18：Retry 2～16 全部 flat tuple arity；每槽初值由类型决定，交付值 = 初值推进一格。
retry_arity_case!(retry_arity_2, 2, s0: u8, s1: u16);
retry_arity_case!(retry_arity_3, 3, s0: u8, s1: u16, s2: u32);
retry_arity_case!(retry_arity_4, 4, s0: u8, s1: u16, s2: u32, s3: u64);
retry_arity_case!(retry_arity_5, 5, s0: u8, s1: u16, s2: u32, s3: u64, s4: u128);
retry_arity_case!(
    retry_arity_6,
    6,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize
);
retry_arity_case!(
    retry_arity_7,
    7,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8
);
retry_arity_case!(
    retry_arity_8,
    8,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16
);
retry_arity_case!(
    retry_arity_9,
    9,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32
);
retry_arity_case!(
    retry_arity_10,
    10,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64
);
retry_arity_case!(
    retry_arity_11,
    11,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128
);
retry_arity_case!(
    retry_arity_12,
    12,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize
);
retry_arity_case!(
    retry_arity_13,
    13,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool
);
retry_arity_case!(
    retry_arity_14,
    14,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool,
    s13: char
);
retry_arity_case!(
    retry_arity_15,
    15,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool,
    s13: char,
    s14: f32
);
retry_arity_case!(
    retry_arity_16,
    16,
    s0: u8,
    s1: u16,
    s2: u32,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool,
    s13: char,
    s14: f32,
    s15: f64
);

// ---------------------------------------------------------------- Iter arity

/// 生成一个 Iter flat-tuple arity 样本：首槽为 u32（0 起），每轮推进全部槽；首槽到 1 时
/// 于下一轮 `IterBreak`，交付进入该轮时的 current（= 各槽初值推进一格）。
macro_rules! iter_arity_case {
    ($name:ident, $s0:ident, $( $slot:ident : $t:ty ),+ $(,)?) => {
        #[test]
        fn $name() {
            let runtime = Runtime::new();
            let outcome = block_on(runtime.execute(
                |flow: &mut Flow<'_>, refs| {
                    let ($s0, $($slot,)+) = refs;
                    flow.iter(($s0, $($slot,)+), 5, |current, round| {
                        let ($s0, $($slot,)+) = current;
                        let _: () = round.then(&judge_break_after_first_round, $s0);
                        (
                            round.then(bump::<u32>, $s0),
                            $(round.then(bump::<$t>, $slot),)+
                        )
                    })
                },
                (0u32, $(<$t as AritySlot>::BOOT,)+),
            ));
            let ($s0, $($slot,)+) = outcome.expect("iter break delivers the advanced current");
            assert_eq!($s0, 1u32, "slot s0 advanced once");
            $(assert_eq!(
                $slot,
                <$t as AritySlot>::BOOT.bumped(),
                concat!("slot ", stringify!($slot))
            );)+
        }
    };
}

// E08／E18：Iter 单 Ref 状态——一轮 Normal 后 IterBreak，交付推进后的 current（0 -> 1）。
#[test]
fn iter_single_ref_state_advances_then_breaks() {
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow: &mut Flow<'_>, seed: Ref<u32>| {
            flow.iter(seed, 5, |current, round| {
                let _: () = round.then(&judge_break_after_first_round, current);
                round.then(bump::<u32>, current)
            })
        },
        0u32,
    ));
    assert_eq!(
        outcome.expect("iter break delivers the advanced current"),
        1u32
    );
}

// E08／E18：Iter 2～16 全部 flat tuple arity；逐槽断言交付值。
iter_arity_case!(iter_arity_2, s0, s1: u8);
iter_arity_case!(iter_arity_3, s0, s1: u8, s2: u16);
iter_arity_case!(iter_arity_4, s0, s1: u8, s2: u16, s3: u64);
iter_arity_case!(iter_arity_5, s0, s1: u8, s2: u16, s3: u64, s4: u128);
iter_arity_case!(iter_arity_6, s0, s1: u8, s2: u16, s3: u64, s4: u128, s5: usize);
iter_arity_case!(
    iter_arity_7,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8
);
iter_arity_case!(
    iter_arity_8,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16
);
iter_arity_case!(
    iter_arity_9,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32
);
iter_arity_case!(
    iter_arity_10,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64
);
iter_arity_case!(
    iter_arity_11,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128
);
iter_arity_case!(
    iter_arity_12,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize
);
iter_arity_case!(
    iter_arity_13,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool
);
iter_arity_case!(
    iter_arity_14,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool,
    s13: char
);
iter_arity_case!(
    iter_arity_15,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool,
    s13: char,
    s14: f32
);
iter_arity_case!(
    iter_arity_16,
    s0,
    s1: u8,
    s2: u16,
    s3: u64,
    s4: u128,
    s5: usize,
    s6: i8,
    s7: i16,
    s8: i32,
    s9: i64,
    s10: i128,
    s11: isize,
    s12: bool,
    s13: char,
    s14: f32,
    s15: f64
);

// ---------------------------------------------------------------- Box Data 叶子

/// 产生一个 `Box<(u8, u16)>`，作为单个 Data 叶子。
async fn boxed_pair(query: Query<()>) -> Result<Box<(u8, u16)>, BodyError> {
    query.get();
    Ok(Box::new((7, 9)))
}

/// 消费单个 `Box<(u8, u16)>` 叶子（Input 为单个 Data，而非两个位置）。
// `&Box<(u8, u16)>` 是本样本要验证的“单个业务叶子”语义：`Box` 是业务 `Data` 的
// 单槽类型，整个 Box（而非其内两元组）作为该槽的物理实例；拆成 `&(u8, u16)` 会
// 把它错误降维成两个位置，背离被冻结的 Input 语义。
#[allow(clippy::borrowed_box)]
async fn sum_boxed(query: Query<&Box<(u8, u16)>>) -> Result<u32, BodyError> {
    let pair = query.get();
    Ok(pair.0 as u32 + pair.1 as u32)
}

// E02：`Box<(A, B)>` 作为 Retry 单输出（单个 Data 叶子）正例；下游以单输入消费。
#[test]
fn retry_boxed_tuple_is_a_single_data_leaf() {
    let runtime = Runtime::new();
    let outcome = block_on(runtime.execute(
        |flow: &mut Flow<'_>, _unit: ()| {
            flow.retry(1, |attempt| {
                let pair = attempt.then(&boxed_pair, ());
                attempt.then(&sum_boxed, pair)
            })
        },
        (),
    ));
    assert_eq!(
        outcome.expect("boxed tuple flows as one leaf"),
        16u32,
        "7 + 9 from the single Box<(u8, u16)> leaf"
    );
}
