use srflow_public_api_v21_probe::*;
struct N;
impl Node for N{type Input=u32;type Output=u32;async fn run(&self,q:Query<&Self::Input>)->Result<u32,BodyError>{Ok(*q.get())}}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|{let n=N;f.then(&n,a)},3u32);}
