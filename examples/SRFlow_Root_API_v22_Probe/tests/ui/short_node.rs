use srflow::{Node,Query,BodyError,Runtime};
struct Service;
impl Node for Service{type Input=();type Output=();async fn run(&self,_:Query<&()>)->Result<(),BodyError>{Ok(())}}
async fn run(){let _value:Result<(),_>=Runtime::new().execute(|f,_|{let local=Service;f.then(&local);},()).await;}
fn main(){}
