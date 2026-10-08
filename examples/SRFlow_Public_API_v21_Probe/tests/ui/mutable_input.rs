use srflow_public_api_v21_probe::*;
async fn mutate(q:Query<&mut u32>)->Result<(),BodyError>{*q.get()+=1;Ok(())}
fn main(){}
