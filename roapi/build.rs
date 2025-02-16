use std::env;
use std::process::Command;

fn main() {
    if env::var("CARGO_FEATURE_UI").is_ok() {
        let manifest_dir = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        let manifest_path = std::path::Path::new(&manifest_dir);
        let workspace_root = manifest_path
            .parent()
            .expect("Failed to extract workspace root");
        let ui_crate_path = workspace_root.join("roapi-ui");

        for entry in &["src", "Cargo.toml", "assets", "index.html", "trunk.toml"] {
            println!(
                "cargo:rerun-if-changed={}/{}",
                ui_crate_path.display(),
                entry
            );
        }

        let output = Command::new("trunk")
            .current_dir(ui_crate_path)
            .arg("build")
            .arg("--release")
            .output()
            .expect("Failed to run Trunk build command");

        if !output.status.success() {
            println!("Trunk build failed");
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            std::process::exit(1);
        }
    }
}
