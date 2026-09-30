//! 正向对照：Each 的集合元素类型与 Body 的 Item 一致。
use srflow::{Each, ExecutionError, FlowBuilder, Node};

struct Item;
impl Node for Item {
    type Input = String;
    type Output = usize;
    async fn run(&self, input: String) -> Result<usize, ExecutionError> {
        Ok(input.len())
    }
}

fn build() {
    let mut flow = FlowBuilder::<Vec<String>>::new();
    let input = flow.input();
    let lengths = flow.then_move(Each::new(Item), input).unwrap();
    let _ = flow.output(lengths).unwrap();
}
