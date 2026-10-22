use futures::executor::block_on;
use srflow::probe::{Flow, Runtime, Workflow};
use srflow::{BodyError, Data, Node, Query};

#[derive(Data, Debug)]
struct Request(u32);
#[derive(Data)]
struct Config(u32);
#[derive(Data, Debug)]
struct Report(u32);
impl Config {
    fn new() -> Self {
        Self(2)
    }
}
async fn generate(q: Query<(&Request, &Config)>) -> Result<Report, BodyError> {
    let (r, c) = q.get();
    Ok(Report(r.0 * c.0))
}
struct Check;
impl Node for Check {
    type Input = Report;
    type Output = ();
    async fn run(&self, q: Query<&Self::Input>) -> Result<(), BodyError> {
        assert!(q.get().0 > 0);
        Ok(())
    }
}
#[allow(clippy::needless_borrows_for_generic_args)] // Verify the requested &function spelling.
fn build_business(flow: &mut Flow) {
    flow.provide(Config::new).then(&generate).then(Check);
    flow.output::<Report>();
}
fn main() {
    let workflow: Workflow<Request, Report> = Flow::build(|flow, _input| {
        flow.chain(build_business);
        flow.output::<Report>();
    });
    assert!(workflow.validate().is_ok());
    let rt = Runtime::new();
    assert_eq!(block_on(rt.execute(&workflow, Request(21))).unwrap().0, 42);
    assert_eq!(block_on(rt.execute(&workflow, Request(10))).unwrap().0, 20);
    println!("build once; execute twice: 42, 20");
}
