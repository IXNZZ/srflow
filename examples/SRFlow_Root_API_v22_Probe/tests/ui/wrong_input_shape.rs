use srflow::Runtime;
async fn run(){let _value:Result<(),_>=Runtime::new().execute(|_f,(_a,_b)|{},1u32).await;}
fn main(){}
