use srflow::probe::{Flow,Workflow};
fn main(){let _:Workflow<(),()>=Flow::build(|f,_|{f.provide(async ||1u32);});}
