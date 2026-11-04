use std::path::Path;

pub fn generate_test_traces(
    spec_rel_path: &str,
    gen_dir: &str,
    quint_seed: u64,
    test_match: Option<&str>,
) {
    println!("🪄 Generating test traces for {spec_rel_path:?}...");

    let spec_abs_path = format!("{}/specs/{spec_rel_path}", env!("CARGO_MANIFEST_DIR"),);
    let spec_path = Path::new(&spec_abs_path);

    let mut command = std::process::Command::new("quint");
    command
        .arg("test")
        .arg("--out-itf")
        .arg(format!("{gen_dir}/test_{{test}}_{{seq}}.itf.json"))
        .arg("--seed")
        .arg(quint_seed.to_string());
    if let Some(test_match) = test_match {
        command.arg("--match").arg(test_match);
    }

    let output = command
        .arg(spec_path)
        .current_dir(spec_path.parent().unwrap())
        .output()
        .expect("Failed to run quint test");

    assert!(
        output.status.success(),
        "quint test failed with status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    println!("🪄 Generated traces in {gen_dir:?}");
}

// pub fn generate_random_traces(
//     spec_rel_path: &str,
//     gen_dir: &str,
//     quint_seed: u64,
//     num_traces: u64,
// ) {
//     println!("🪄 Generating random traces for {spec_rel_path:?}...");

//     let spec_abs_path = format!("{}/specs/{spec_rel_path}", env!("CARGO_MANIFEST_DIR"),);
//     let spec_path = Path::new(&spec_abs_path);

//     std::process::Command::new("quint")
//         .arg("run")
//         .arg("--n-traces")
//         .arg(num_traces.to_string())
//         .arg("--max-samples")
//         .arg("1000")
//         .arg("--out-itf")
//         .arg(format!("{gen_dir}/random_{{seq}}.itf.json"))
//         .arg("--seed")
//         .arg(quint_seed.to_string())
//         .arg(spec_path)
//         .current_dir(spec_path.parent().unwrap())
//         .output()
//         .expect("Failed to run quint test");

//     println!("🪄 Generated traces in {gen_dir:?}");
// }

const DEFAULT_QUINT_SEED: u64 = 118;

pub fn quint_seed() -> u64 {
    let seed = std::env::var("QUINT_SEED")
        .ok()
        .and_then(|x| x.parse::<u64>().ok())
        .unwrap_or(DEFAULT_QUINT_SEED);

    println!("Using QUINT_SEED={seed}");

    seed
}
