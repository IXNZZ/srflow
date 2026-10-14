use srflow::legacy::FlowBuilder;

fn main() {
    let _ = FlowBuilder::<(
        u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8,
    )>::start();
}
