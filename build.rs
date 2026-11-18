fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(false)
        .generate_default_stubs(true)
        .include_file("mod.rs")
        .compile_protos(&["proto/externalgrpc.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto");
    Ok(())
}
