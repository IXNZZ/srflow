struct Query<T>(T);
trait Input {type Borrowed<'a>;}
impl Input for (u32,u32){type Borrowed<'a>=(&'a u32,&'a u32);}
trait Node{type Input:Input;async fn run<'a>(&self,q:Query<<Self::Input as Input>::Borrowed<'a>>)->u32;}
struct Add;
impl Node for Add{type Input=(u32,u32);async fn run<'a>(&self,q:Query<(&'a u32,&'a u32)>)->u32{let(a,b)=q.0;a+b}}
fn main(){}
