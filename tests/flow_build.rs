//! T02 验收：Flow 构建期的接线检查。
//!
//! 覆盖：跨 Flow 的 `Ref`（即使位置编号与类型都相同）、重复取走同一个位置、失败后的构建器
//! 状态，以及强类型连接的编译期检查。这些测试只使用 crate 的公开 API。

use futures::executor::block_on;
use srflow::{Executable, ExecutionError, Flow, FlowBuildError, FlowBuilder, Node, Runtime};

/// 叶子：返回输入文本的字符数。
struct Length;

impl Node for Length {
    type Input = String;
    type Output = usize;

    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.chars().count())
    }
}

/// 叶子：把数值翻倍。
struct Double;

impl Node for Double {
    type Input = usize;
    type Output = usize;

    async fn run(&self, input: usize) -> Result<usize, ExecutionError> {
        Ok(input * 2)
    }
}

#[test]
fn foreign_ref_with_same_slot_and_type_is_rejected_by_then() {
    let mut owner = FlowBuilder::<String>::new();
    let owned_input = owner.input();

    let mut other = FlowBuilder::<String>::new();
    // 两个 Flow 的 Input 位置编号相同、类型都是 Ref<String>，仍必须被拒绝。
    let error = other.then_move(Length, owned_input).unwrap_err();
    assert_eq!(error, FlowBuildError::ForeignRef);
    assert!(error.to_string().contains("另一个 Flow"));

    // 归属 Flow 自己仍然可以正常使用这个 Ref。
    let length = owner.then_move(Length, owned_input).unwrap();
    let flow = owner.output(length).unwrap();
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap(),
        4
    );
}

#[test]
fn foreign_ref_is_rejected_by_output() {
    let owner = FlowBuilder::<String>::new();
    let owned_input = owner.input();

    let other = FlowBuilder::<String>::new();
    let error = other.output(owned_input).unwrap_err();
    assert_eq!(error, FlowBuildError::ForeignRef);
}

#[test]
fn consuming_the_same_source_twice_is_rejected() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow.then_move(Length, input).unwrap();

    let error = flow.then_move(Length, input).unwrap_err();
    assert_eq!(error, FlowBuildError::SourceAlreadyConsumed);
    assert!(error.to_string().contains("已被取走"));

    let flow = flow.output(length).unwrap();
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("abc"))).unwrap(),
        3
    );
}

#[test]
fn consumed_source_cannot_become_the_output() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let _length = flow.then_move(Length, input).unwrap();

    // `output` 也消费位置：已经被 `then_move` 取走的值不能再作为最终 Output。
    let error = flow.output(input).unwrap_err();
    assert_eq!(error, FlowBuildError::SourceAlreadyConsumed);
}

#[test]
fn shared_reads_keep_the_source_usable_and_output_may_be_a_non_final_step() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();

    // 同一个位置被两次复用读取：`then` 不会消费它。
    let first = flow.then(Length, input).unwrap();
    let second = flow.then(Length, input).unwrap();
    let _doubled = flow.then_move(Double, second).unwrap();

    // Output 可以选非最后一步产生的值。
    let flow = flow.output(first).unwrap();
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap(),
        4
    );
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("ab"))).unwrap(),
        2
    );
}

#[test]
fn builder_stays_usable_after_a_rejected_connection() {
    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();

    let other = FlowBuilder::<String>::new();
    let foreign = other.input();
    assert!(flow.then_move(Length, foreign).is_err());

    // `then`／`then_move` 失败只拒绝该次连接，构建器仍然可用。
    let length = flow.then_move(Length, input).unwrap();
    let flow = flow.output(length).unwrap();
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("ab"))).unwrap(),
        2
    );
}

#[test]
fn built_flow_declares_a_strongly_typed_contract() {
    fn assert_executable<E: Executable<Input = String, Output = usize>>() {}
    assert_executable::<Flow<String, usize>>();

    let mut flow = FlowBuilder::<String>::new();
    let input = flow.input();
    let length = flow.then_move(Length, input).unwrap();
    let flow: Flow<String, usize> = flow.output(length).unwrap();
    assert_eq!(
        block_on(Runtime::new().execute(&flow, String::from("abcd"))).unwrap(),
        4
    );
}
