use srflow::probe::{Flow,Workflow};
fn main(){let _:Workflow<(),()>=Flow::build(|f,_|{f.chain(|s|s.provide_output(||1u32));});}
