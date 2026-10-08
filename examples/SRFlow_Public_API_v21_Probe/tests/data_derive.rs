use futures::executor::block_on;
use srflow_public_api_v21_probe::{Data, Runtime};
use std::{cell::Cell, rc::Rc};

// This field type deliberately has no Data / Clone / Copy implementation.
struct BusinessField(Rc<Cell<u32>>);

#[derive(Data)]
struct Record {
    field: BusinessField,
}

#[derive(Data)]
enum Event {
    Created(BusinessField),
    Empty,
}

#[derive(Data)]
struct Generic<T, const N: usize>
where
    T: AsRef<str>,
{
    value: T,
    bytes: [u8; N],
}

#[derive(Data)]
struct StaticText<'a>(&'a str);

#[test]
fn derive_record_needs_no_field_data_clone_copy_send_or_sync() {
    let cell = Rc::new(Cell::new(7));
    let result = block_on(Runtime::new().execute(
        |_, input| input,
        Record {
            field: BusinessField(cell.clone()),
        },
    ))
    .unwrap();
    assert!(Rc::ptr_eq(&result.field.0, &cell));
    result.field.0.set(8);
    assert_eq!(cell.get(), 8);
}

#[test]
fn derive_enum_is_one_data_leaf() {
    let cell = Rc::new(Cell::new(9));
    let result = block_on(Runtime::new().execute(
        |_, input| input,
        Event::Created(BusinessField(cell.clone())),
    ))
    .unwrap();
    match result {
        Event::Created(field) => assert!(Rc::ptr_eq(&field.0, &cell)),
        Event::Empty => panic!("wrong business variant"),
    }
    assert!(matches!(
        block_on(Runtime::new().execute(|_, input| input, Event::Empty)).unwrap(),
        Event::Empty
    ));
}

#[test]
fn derive_preserves_generic_where_const_and_static_lifetime_support() {
    let (generic, text) = block_on(Runtime::new().execute(
        |_, input| input,
        (
            Generic {
                value: String::from("generic"),
                bytes: [1, 2, 3],
            },
            StaticText("static"),
        ),
    ))
    .unwrap();
    assert_eq!(generic.value, "generic");
    assert_eq!(generic.bytes, [1, 2, 3]);
    assert_eq!(text.0, "static");
}
