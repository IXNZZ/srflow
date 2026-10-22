use srflow::{BodyError,Query};
use srflow::probe::{Flow,Workflow};
async fn escape(q:Query<&String>)->Result<&str,BodyError>{Ok(q.get().as_str())}
fn main(){let _:Workflow<String,()>=Flow::build(|f,_|{f.then(escape);});}
