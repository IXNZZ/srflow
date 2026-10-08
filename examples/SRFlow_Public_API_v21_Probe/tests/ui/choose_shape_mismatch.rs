use srflow_public_api_v21_probe::*;
async fn text(_:Query<&u32>)->Result<String,BodyError>{Ok("text".into())}
fn main(){let r=Runtime::new();let _=r.execute(|f,(route,a)|f.choose(route,|c|{c.case(0u32,|_|a);c.case(1u32,|s|s.then(text,a));}),(0u32,3u32));}
