use srflow_public_api_v21_probe::*;
async fn sum(q:Query<(&u32,&u32)>)->Result<u32,BodyError>{let(a,b)=q.get();Ok(a+b)}
fn main(){let r=Runtime::new();let _=r.execute(|f,a|f.then(sum,a),3u32);}
