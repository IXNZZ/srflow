use srflow::{Query,BodyError,Runtime,RunError};
async fn add(q:Query<(&u32,&u32)>)->Result<u32,BodyError>{let(a,b)=q.get();Ok(*a+*b)}
async fn run(rt:&Runtime)->Result<u32,RunError>{let n:u32=rt.execute(|f,(a,b)|{f.then_with(add,(a,b));f.output::<u32>();},(3u32,9u32)).await?;Ok(n)}
fn main(){assert_eq!(futures::executor::block_on(run(&Runtime::new())).unwrap(),12);}
