use srflow::{BodyError,Query};
use srflow::probe::{Flow,Workflow};
async fn number(q:Query<&u32>)->Result<u32,BodyError>{Ok(*q.get())}
fn main(){let _:Workflow<String,()>=Flow::build(|f,s|{f.then_with(number,s);});}
