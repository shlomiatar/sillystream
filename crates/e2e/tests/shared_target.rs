//! Several suites on one target directory, as every worktree of a workflow has. Each must run
//! the binaries it built, see only its own servers, and not disturb the others.

use std::process::Command;

use e2e::{Node, NodeOptions, binaries, servers_running};

const CHILD_ENV: &str = "E2E_SHARED_TARGET_CHILD";
const CHILD_COUNT: usize = 8;
const ROUND_COUNT: usize = 3;
const TEST_NAME: &str = "concurrent_runs_on_one_target_directory_do_not_disturb_each_other";

/// What each process does: start from the same binaries, see no server but its own, and stop.
fn child() {
    for _ in 0..ROUND_COUNT {
        assert!(binaries().server.is_file());
        let mut node = Node::started(NodeOptions::default());
        let ours = servers_running();
        assert_eq!(ours.len(), 1, "only this run's server: {ours:?}");
        node.stop();
        let left = servers_running();
        assert!(left.is_empty(), "only this run's servers: {left:?}");
    }
}

#[test]
fn concurrent_runs_on_one_target_directory_do_not_disturb_each_other() {
    if std::env::var_os(CHILD_ENV).is_some() {
        child();
        return;
    }
    let exe = std::env::current_exe().expect("this test's binary");
    // All at once, so that their builds, copies and starts overlap.
    let children: Vec<_> = (0..CHILD_COUNT)
        .map(|_| {
            let exe = exe.clone();
            std::thread::spawn(move || {
                Command::new(exe)
                    .args(["--exact", TEST_NAME, "--nocapture"])
                    .env(CHILD_ENV, "1")
                    .output()
                    .expect("running a child")
            })
        })
        .collect();
    let mut failures = Vec::new();
    for child in children {
        let output = child.join().expect("a thread that returns");
        if !output.status.success() {
            failures.push(String::from_utf8_lossy(&output.stderr).into_owned());
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {CHILD_COUNT} failed, the first: {}",
        failures.len(),
        failures[0]
    );
}
