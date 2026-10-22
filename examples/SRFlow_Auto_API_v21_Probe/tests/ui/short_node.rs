use srflow::{Node,Query,BodyError};
use srflow::probe::{Flow,Workflow};
struct Service;
impl Node for Service{type Input=();type Output=();async fn run(&self,_:Query<&()>)->Result<(),BodyError>{Ok(())}}
fn main(){let w:Workflow<(),()>=Flow::build(|f,_|{let local=Service;f.then(&local);});let _=w.validate();}
