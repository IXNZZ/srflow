use futures::executor::block_on;
use srflow_public_api_v21_probe::*;
use std::{cell::Cell, rc::Rc};

#[derive(Data)]
struct Loaded(String);

async fn load_data(q: Query<()>) -> Result<Loaded, BodyError> {
    let () = q.get();
    Ok(Loaded(String::from("loaded")))
}

#[test]
fn zero_root_user_example_loads_owned_data() {
    block_on(async {
        let runtime = Runtime::new();
        let result = runtime
            .execute(|flow, ()| flow.then(&load_data, ()), ())
            .await
            .unwrap();
        assert_eq!(result.0, "loaded");
    });
}

struct Load;
impl Node for Load {
    type Input = ();
    type Output = Loaded;
    async fn run(&self, q: Query<&Self::Input>) -> Result<Loaded, BodyError> {
        let () = q.get();
        Ok(Loaded(String::from("struct")))
    }
}

#[test]
fn zero_root_struct_source_supports_chain_and_multiple_outputs() {
    block_on(async {
        let runtime = Runtime::new();
        let load = Load;
        async fn size(q: Query<&Loaded>) -> Result<usize, BodyError> {
            Ok(q.get().0.len())
        }
        let (loaded, size) = runtime
            .execute(
                |flow, ()| {
                    let loaded = flow.chain(|sub| sub.then(&load, ()));
                    let size = flow.then(size, loaded);
                    (loaded, size)
                },
                (),
            )
            .await
            .unwrap();
        assert_eq!(loaded.0, "struct");
        assert_eq!(size, 6);
    });
}

#[derive(Data)]
struct Tracked(Rc<Cell<usize>>);
impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn zero_root_empty_and_unit_output_execute_and_release_unused_data() {
    block_on(async {
        let runtime = Runtime::new();
        runtime.execute(|_, ()| (), ()).await.unwrap();
        let calls = Cell::new(0);
        let drops = Rc::new(Cell::new(0));
        let load = async |q: Query<()>| -> Result<Tracked, BodyError> {
            let () = q.get();
            Ok(Tracked(drops.clone()))
        };
        let step = async |q: Query<()>| -> Result<(), BodyError> {
            let () = q.get();
            calls.set(calls.get() + 1);
            Ok(())
        };
        runtime
            .execute(
                |flow, ()| {
                    flow.then(load, ());
                    flow.then(step, ());
                },
                (),
            )
            .await
            .unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(drops.get(), 1);
    });
}

#[test]
fn zero_root_keeps_retry_boundaries_and_unhandled_control() {
    block_on(async {
        let runtime = Runtime::new();
        let calls = Cell::new(0);
        let load = async |q: Query<()>| -> Result<Loaded, BodyError> {
            let () = q.get();
            calls.set(calls.get() + 1);
            if calls.get() == 1 {
                Err(BodyError::retry("temporary"))
            } else {
                Ok(Loaded(String::from("retry")))
            }
        };
        let result = runtime
            .execute(
                |flow, ()| flow.retry(1, |attempt| attempt.then(load, ())),
                (),
            )
            .await
            .unwrap();
        assert_eq!(result.0, "retry");
        assert_eq!(calls.get(), 2);
        async fn stop(_: Query<()>) -> Result<(), BodyError> {
            Err(BodyError::iter_break())
        }
        assert!(matches!(
            runtime.execute(|flow, ()| flow.then(stop, ()), ()).await,
            Err(RunError::UnhandledControl(ControlSignal::IterBreak(_)))
        ));
    });
}

#[test]
fn zero_root_failure_releases_loaded_data_and_skips_later_steps() {
    block_on(async {
        let runtime = Runtime::new();
        let drops = Rc::new(Cell::new(0));
        let calls = Cell::new(0);
        let load =
            async |_: Query<()>| -> Result<Tracked, BodyError> { Ok(Tracked(drops.clone())) };
        async fn fail(_: Query<&Tracked>) -> Result<(), BodyError> {
            Err(BodyError::fail(std::io::Error::other("load failed")))
        }
        let later = async |_: Query<()>| -> Result<(), BodyError> {
            calls.set(1);
            Ok(())
        };
        let result = runtime
            .execute(
                |flow, ()| {
                    let data = flow.then(load, ());
                    flow.then(fail, data);
                    flow.then(later, ());
                },
                (),
            )
            .await;
        assert!(matches!(result, Err(RunError::Body(_))));
        assert_eq!(drops.get(), 1);
        assert_eq!(calls.get(), 0);
    });
}
