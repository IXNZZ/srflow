use std::{fs, path::PathBuf, process::Command};
#[test]
fn r18_root_consumer_compile_contracts() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let lib = fs::read_dir(&deps)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("libsrflow-")
                && p.extension().is_some_and(|x| x == "rlib")
        })
        .max_by_key(|p| p.metadata().unwrap().modified().unwrap())
        .expect("built library");
    let output = root.join("target/auto_ui");
    fs::create_dir_all(&output).unwrap();
    let futures_lib = fs::read_dir(&deps)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("libfutures-")
                && p.extension().is_some_and(|x| x == "rlib")
        })
        .max_by_key(|p| p.metadata().unwrap().modified().unwrap())
        .unwrap();
    let cases = [
        ("no_expected_output", "E0283"),
        ("wrong_input_shape", "E0308"),
        ("body_return_ref", "E0308"),
        ("no_workflow", "E0432"),
        ("no_flow_build", "E0599"),
        ("short_node", "E0597"),
        ("positive", ""),
    ];
    for (name, code) in cases {
        let result = Command::new("rustc")
            .arg("--edition=2024")
            .arg("--crate-type=bin")
            .arg(root.join(format!("tests/ui/{name}.rs")))
            .arg("--extern")
            .arg(format!("srflow={}", lib.display()))
            .arg("--extern")
            .arg(format!("futures={}", futures_lib.display()))
            .arg("-L")
            .arg(format!("dependency={}", deps.display()))
            .arg("-o")
            .arg(output.join(name))
            .output()
            .unwrap();
        let stderr = String::from_utf8(result.stderr).unwrap();
        fs::write(output.join(format!("{name}.stderr")), &stderr).unwrap();
        println!("{name}: {} {}", result.status, code);
        if code.is_empty() {
            assert!(result.status.success(), "{name}: {stderr}");
            assert!(Command::new(output.join(name)).status().unwrap().success());
        } else {
            assert!(!result.status.success(), "{name} unexpectedly accepted");
            assert!(stderr.contains(code), "{name}: expected {code}: {stderr}");
        }
    }
}
