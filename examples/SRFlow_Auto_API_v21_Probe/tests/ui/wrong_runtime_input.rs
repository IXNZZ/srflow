use srflow::probe::{Flow,Workflow,Runtime};
fn main(){let w:Workflow<u32,u32>=Flow::build(|f,_|{f.output::<u32>();});let rt=Runtime::new();let _future=rt.execute(&w,String::new());}
