use std::process::Command;

fn main() {
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok());

    if let Some(commit) = commit {
        println!("cargo:rustc-env=BUILD_GIT_COMMIT={}", commit.trim());
    }
}
