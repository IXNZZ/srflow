//! 正向对照：Match 分支的共同 Output 与 Match 的 `O` 一致。
use srflow::{ExecutionError, Match, Node};

#[derive(Debug, PartialEq, Eq)]
enum Route {
    A,
}

struct Branch;
impl Node for Branch {
    type Input = String;
    type Output = Vec<String>;
    async fn run(&self, input: String) -> Result<Vec<String>, ExecutionError> {
        Ok(vec![input])
    }
}

fn build() {
    let mut builder = Match::<Route, String, Vec<String>>::builder();
    builder.case(Route::A, Branch).unwrap();
    let _ = builder.build();
}
