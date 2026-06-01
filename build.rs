fn main() {
    if std::env::var("CARGO_CFG_WINDOWS").is_ok() {
        // DuckDB bundled build on Windows requires the Restart Manager API
        println!("cargo:rustc-link-lib=dylib=Rstrtmgr");
    }
}
