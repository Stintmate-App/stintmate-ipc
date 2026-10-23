//! The key file on disk: atomic creation, the user-only DACL and re-reading on change. Only
//! temporary folders: never the driver's key.

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use stintmate_ipc::error::KeyFileError;
use stintmate_ipc::keyfile::{
    KeyFile, KeySource, default_key_path, ensure_key, format_key, read_key,
};
use stintmate_ipc::naming::new_run_id;
use stintmate_ipc::security;

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "stintmate-ipc-{tag}-{}-{}",
        std::process::id(),
        new_run_id()
    ));
    fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn ensure_key_creates_once_and_reuses() {
    let d = temp_dir("key-create");
    let path = d.join("ipc").join("key");
    let k1 = ensure_key(&path).unwrap();
    let text = fs::read(&path).unwrap();
    assert_eq!(text, format_key(&k1));
    assert_eq!(text.len(), 66);
    assert!(text.ends_with(b"\r\n"));
    assert_eq!(ensure_key(&path).unwrap(), k1);
    assert_eq!(read_key(&path).unwrap(), k1);
    // no temporary file is left behind
    assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    let _ = fs::remove_dir_all(d);
}

#[test]
fn concurrent_creators_agree_on_one_key() {
    for round in 0..5 {
        let d = temp_dir(&format!("key-race{round}"));
        let path = Arc::new(d.join("key"));
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let (path, barrier) = (path.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    ensure_key(&path).unwrap()
                })
            })
            .collect();
        let keys: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        assert!(
            keys.iter().all(|k| *k == keys[0]),
            "all creators must read the winner's key"
        );
        assert_eq!(read_key(&path).unwrap(), keys[0]);
        assert_eq!(
            fs::read_dir(&d).unwrap().count(),
            1,
            "temporary files cleaned up"
        );
        let _ = fs::remove_dir_all(d);
    }
}

#[test]
fn the_key_file_is_for_the_current_user_only() {
    let d = temp_dir("key-dacl");
    let path = d.join("key");
    ensure_key(&path).unwrap();
    let sid = security::current_user_sid().unwrap();
    let sddl = security::dacl_sddl_of_path(&path).unwrap();
    // protected (no inheritance) and one ACE: the current user
    assert!(sddl.starts_with("D:P"), "{sddl}");
    assert_eq!(sddl.matches("(A;").count(), 1, "{sddl}");
    // SDDL writes the built-in Administrator (RID 500, e.g. a CI runner's user) as its alias "LA"
    let alias_ok = sid.ends_with("-500") && sddl.contains(";LA)");
    assert!(
        sddl.contains(&format!(";{sid})")) || alias_ok,
        "{sddl} for {sid}"
    );
    assert!(security::user_only_sddl("FA").unwrap().contains(&sid));
    let _ = fs::remove_dir_all(d);
}

#[test]
fn a_key_file_is_re_read_when_it_changes() {
    let d = temp_dir("key-reread");
    let path = d.join("key");
    let src = KeySource::File(KeyFile::create(&path));
    let k1 = src.key().unwrap();
    assert_eq!(src.key().unwrap(), k1);
    // same size, new content: wait out the coarsest timestamp granularity
    std::thread::sleep(Duration::from_millis(50));
    let k2 = [0x42u8; 32];
    fs::write(&path, format_key(&k2)).unwrap();
    assert_eq!(src.key().unwrap(), k2);
    // a different size (no line end) is noticed as well
    fs::write(&path, hex_of(&[0x07; 32])).unwrap();
    assert_eq!(src.key().unwrap(), [0x07; 32]);
    // an invalid file is an error naming the file, never the content
    fs::write(&path, "zz").unwrap();
    let err = src.key().unwrap_err();
    assert!(
        matches!(&err, KeyFileError::Invalid { path: p, .. } if p.ends_with("key")),
        "{err}"
    );
    let _ = fs::remove_dir_all(d);
}

fn hex_of(k: &[u8; 32]) -> String {
    k.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn a_missing_file_is_an_error_unless_creating() {
    let d = temp_dir("key-missing");
    let path = d.join("key");
    let err = KeySource::File(KeyFile::open(&path)).key().unwrap_err();
    assert!(matches!(err, KeyFileError::NotFound(_)));
    assert!(!path.exists());
    assert!(KeySource::File(KeyFile::create(&path)).key().is_ok());
    assert!(path.exists());
    assert_eq!(KeySource::Bytes([1; 32]).key().unwrap(), [1; 32]);
    // Debug never shows a key
    assert!(!format!("{:?}", KeySource::Bytes([0xab; 32])).contains("ab"));
    let _ = fs::remove_dir_all(d);
}

#[test]
fn default_path_is_under_the_workspace() {
    let p = default_key_path(Some(std::path::Path::new(r"C:\Work\Ws"))).unwrap();
    assert_eq!(p, PathBuf::from(r"C:\Work\Ws\ipc\key"));
}
