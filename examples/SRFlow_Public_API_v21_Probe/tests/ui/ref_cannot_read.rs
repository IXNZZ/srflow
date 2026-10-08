use srflow_public_api_v21_probe::*;
fn main(){let r=Runtime::new();let _=r.execute(|_,a|{a.get();a},3u32);}
