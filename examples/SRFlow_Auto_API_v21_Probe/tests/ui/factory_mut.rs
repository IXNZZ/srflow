use srflow::probe::{Flow,Workflow};
fn main(){let mut count=0;let _:Workflow<(),u32>=Flow::build(|f,_|{f.provide(move||{count+=1;count});f.output::<u32>();});}
