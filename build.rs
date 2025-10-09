fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&["proto/text_bank.proto"], &["proto"])
        .unwrap();
}