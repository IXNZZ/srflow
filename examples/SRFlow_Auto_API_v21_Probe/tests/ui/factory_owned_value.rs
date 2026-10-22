use srflow::probe::{Flow,Workflow};
fn main(){let _:Workflow<(),u32>=Flow::build(|f,_|{f.provide(3u32);f.output::<u32>();});}
