// Names the macOS shared library by @rpath, so a program finds it wherever
// the program's rpath says it is installed.
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-install_name,@rpath/liblionturtle_c.dylib");
    }
}
