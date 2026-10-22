use srflow::probe::{Flow,Workflow};
fn main(){let _:Workflow<(),u32>=Flow::build(|f,_|{f.output_with(f.provide_output(||1u32));});}
