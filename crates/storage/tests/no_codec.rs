//! Enforces that `storage` and `server` have no compression codec in their dependency
//! trees, with every edge kind on every target (SPEC invariants, D19).

use std::process::Command;

const BANNED: [&str; 6] = ["zstd", "lz4", "flate2", "miniz", "snappy", "brotli"];

/// Package names in `cargo tree --prefix none` output containing a banned name.
fn banned_packages(tree: &str) -> Vec<String> {
    let mut found: Vec<String> = tree
        .lines()
        .filter_map(|line| line.split_whitespace().next())
        .filter(|name| {
            let name = name.to_ascii_lowercase();
            BANNED.iter().any(|banned| name.contains(banned))
        })
        .map(str::to_owned)
        .collect();
    found.sort();
    found.dedup();
    found
}

fn tree(package: &str) -> String {
    let output = Command::new(env!("CARGO"))
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args([
            "tree",
            "--offline",
            "--locked",
            "--package",
            package,
            "--edges",
            "normal,build,dev",
            "--target",
            "all",
            "--prefix",
            "none",
        ])
        .output()
        .expect("cargo tree runs");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn storage_has_no_compression_codec() {
    let tree = tree("storage");
    // The scan looked at the tree that matters: these are the likely sources.
    assert!(tree.contains("fjall v"), "{tree}");
    assert!(tree.contains("lsm-tree v"), "{tree}");
    assert!(tree.contains("object_store v"), "{tree}");
    assert_eq!(banned_packages(&tree), Vec::<String>::new());
}

#[test]
fn server_has_no_compression_codec() {
    let tree = tree("server");
    assert!(tree.contains("storage v"), "{tree}");
    assert!(tree.contains("fjall v"), "{tree}");
    assert_eq!(banned_packages(&tree), Vec::<String>::new());
}

#[test]
fn the_check_catches_each_banned_name() {
    let tree = "fjall v3.1.13\nlz4_flex v0.11.0\nzstd-sys v2.0.0 (*)\nflate2 v1.0.0\n\
                miniz_oxide v0.8.0\nsnap v1.0.0\nsnappy-sys v0.1.0\nbrotli v7.0.0\n";
    assert_eq!(
        banned_packages(tree),
        [
            "brotli",
            "flate2",
            "lz4_flex",
            "miniz_oxide",
            "snappy-sys",
            "zstd-sys"
        ]
    );
}
