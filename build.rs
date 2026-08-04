use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Always rebuild metadata; proto compile only when the feature is on.
    println!("cargo:rerun-if-changed=proto/sekai.proto");
    println!("cargo:rerun-if-changed=proto/chisei.proto");
    println!("cargo:rerun-if-changed=build.rs");

    #[cfg(feature = "governance-sekai-chisei")]
    {
        let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("proto");
        let protos = [
            proto_dir.join("sekai.proto"),
            proto_dir.join("chisei.proto"),
        ];
        // SAFETY: build scripts routinely set PROTOC for the compile step only.
        unsafe {
            std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
        }
        tonic_prost_build::configure()
            .build_server(false)
            .build_client(true)
            .compile_protos(&protos, &[proto_dir])?;
    }
    Ok(())
}
