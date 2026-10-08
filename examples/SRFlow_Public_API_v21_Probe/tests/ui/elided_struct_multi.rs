use srflow_public_api_v21_probe::{Node,Query,BodyError};
struct Add;
impl Node for Add {
    type Input=(u32,u32);
    type Output=u32;
    async fn run(&self,q:Query<(&u32,&u32)>)->Result<u32,BodyError>{let(a,b)=q.get();Ok(a+b)}
}
fn main(){}
