use std::{fs, path::PathBuf, process::Command};
#[test]
fn p09_consumer_compile_contracts() {
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
    let cases = [
        ("projection_inference", "E0283"),
        ("factory_once", "E0507"),
        ("factory_mut", "E0594"),
        ("factory_owned_value", "E0277"),
        ("factory_async", "E0277"),
        ("tuple_not_data", "E0277"),
        ("unit_not_ref", "E0277"),
        ("borrow_escape", "E0277"),
        ("wrong_explicit_input", "E0271"),
        ("wrong_runtime_input", "E0308"),
        ("short_node", "E0597"),
        ("body_return_ref", "E0308"),
        ("nested_mut_borrow", "E0499"),
        ("positive", ""),
    ];
    for (name, code) in cases {
        let result = Command::new("rustc")
            .arg("--edition=2024")
            .arg("--crate-type=bin")
            .arg(root.join(format!("tests/ui/{name}.rs")))
            .arg("--extern")
            .arg(format!("srflow={}", lib.display()))
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
