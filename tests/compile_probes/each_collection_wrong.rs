//! 负向：[错误] Each 的 Body 需要 String 元素，父 Flow 的位置是 Vec<u32>。
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
    let mut flow = FlowBuilder::<Vec<u32>>::new();
    let input = flow.input();
    let _ = flow.then_move(Each::new(Item), input); // 此行报错：expected Ref<Vec<String>>, found Ref<Vec<u32>>
}
