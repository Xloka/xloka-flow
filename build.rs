/// build.rs — xloka-flow build script
///
/// On Windows, DuckDB's bundled source references the Restart Manager API
/// (RmStartSession, RmEndSession, RmRegisterResources, RmGetList) to detect
/// which processes hold a lock on the database file.  The linker needs
/// `Rstrtmgr.lib` which is not linked automatically.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        println!("cargo:rustc-link-lib=rstrtmgr");
    }
}
