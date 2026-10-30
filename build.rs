fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows")
        || std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("windows")
    {
        // DuckDB bundled build on Windows requires the Restart Manager API
        println!("cargo:rustc-link-lib=rstrtmgr");
    }
}
