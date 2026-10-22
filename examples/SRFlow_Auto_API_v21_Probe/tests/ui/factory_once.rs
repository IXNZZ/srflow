use srflow::probe::{Flow,Workflow};
fn main(){let owned=String::from("non Copy");let _:Workflow<(),String>=Flow::build(|f,_|{f.provide(move||owned);f.output::<String>();});}
