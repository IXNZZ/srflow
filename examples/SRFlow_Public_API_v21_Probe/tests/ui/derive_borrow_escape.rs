use srflow_public_api_v21_probe::Data;
#[derive(Data)]
struct Borrowed<'a> { value: &'a str }
fn require_data<T: Data>(_: T) {}
fn main() {
    let value = String::from("temporary");
    require_data(Borrowed { value: &value });
}
