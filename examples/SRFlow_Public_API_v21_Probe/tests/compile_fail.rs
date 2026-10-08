use std::{fs, path::Path, process::Command};
#[test]
fn ap_compile_fail_evidence() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_owned();
    let mut libs: Vec<_> = fs::read_dir(&deps)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("libsrflow_public_api_v21_probe-")
                && p.extension().is_some_and(|e| e == "rlib")
        })
        .collect();
    libs.sort_by_key(|p| fs::metadata(p).unwrap().modified().unwrap());
    let lib = libs.last().expect("Cargo-built consumer library");
    let output = root.join("target/ui");
    fs::create_dir_all(&output).unwrap();
    let cases = [
        ("original_gat_elision", vec!["E0195"]),
        ("original_gat_explicit", vec!["E0195"]),
        ("elided_struct_multi", vec!["E0053"]),
        ("wrong_input", vec!["E0631", "E0271"]),
        ("wrong_arity", vec!["E0631", "E0271"]),
        ("by_value_node", vec!["E0631", "E0271", "E0277"]),
        ("choose_shape_mismatch", vec!["E0308"]),
        ("iteration_state_mismatch", vec!["E0308"]),
        ("borrow_escape", vec!["E0277", "E0308"]),
        ("mutable_input", vec!["E0277"]),
        ("private_core", vec!["E0603"]),
        ("ref_cannot_read", vec!["E0599"]),
        ("derive_borrow_escape", vec!["E0597"]),
        ("dyn_node", vec!["E0038"]),
        ("synchronous_node", vec!["E0277"]),
        ("definition_owned_value", vec!["E0277"]),
        ("tuple_leaf_output", vec!["E0277"]),
        ("short_lived_node", vec!["E0597"]),
    ];
    for (name, codes) in cases {
        let result = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .arg("--edition=2024")
            .arg(root.join(format!("tests/ui/{name}.rs")))
            .arg("--extern")
            .arg(format!("srflow_public_api_v21_probe={}", lib.display()))
            .arg("-L")
            .arg(format!("dependency={}", deps.display()))
            .arg("--out-dir")
            .arg(&output)
            .output()
            .unwrap();
        let diagnostic = String::from_utf8(result.stderr).unwrap();
        fs::write(output.join(format!("{name}.stderr")), &diagnostic).unwrap();
        assert!(!result.status.success(), "{name} unexpectedly compiled");
        assert!(
            codes.iter().any(|code| diagnostic.contains(code)),
            "{name}: wrong rejection\n{diagnostic}"
        );
        assert!(
            !diagnostic.contains("E0463"),
            "{name}: missing dependency is not evidence"
        );
        println!(
            "{name}: rejected by {}",
            codes
                .iter()
                .find(|code| diagnostic.contains(**code))
                .unwrap()
        );
    }
}
