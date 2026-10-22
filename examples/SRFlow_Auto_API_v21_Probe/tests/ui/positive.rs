use srflow::{Data,Node,Query,BodyError,Ref};
use srflow::probe::{Flow,Workflow};
#[derive(Data)]struct A(String);
#[derive(Data)]struct B(usize);
async fn convert(q:Query<&A>)->Result<B,BodyError>{Ok(B(q.get().0.len()))}
struct Struct;
impl Node for Struct{type Input=(A,B);type Output=();async fn run(&self,q:Query<&Self::Input>)->Result<(),BodyError>{let _=q.get();Ok(())}}
fn module(f:&mut Flow){f.then(&convert).then(Struct);f.output::<B>();}
fn main(){let w:Workflow<A,B>=Flow::build(|f,_|{let b:Ref<B>=f.chain_output(module);f.output_with(b);});assert!(w.validate().is_ok());}
