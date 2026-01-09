fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = std::path::Path::new("../proto");
    let includes = vec!["../proto/"];

    let proto_paths = [
        proto_root.join("osprey/rpc/actions/v1/action.proto"),
        proto_root.join("osprey/rpc/osprey_coordinator/sync_action/v1/service.proto"),
        proto_root.join("osprey/rpc/osprey_coordinator/bidirectional_stream/v1/service.proto"),
        proto_root.join("osprey/rpc/common/v1/*.proto"),
    ]
    .into_iter()
    .map(|pattern| glob::glob(pattern.to_str().unwrap()))
    .collect::<Result<Vec<_>, _>>()?;

    let source_proto_files = proto_paths
        .into_iter()
        .flat_map(|paths| {
            paths.filter_map(|maybe_path| {
                maybe_path
                    .ok()
                    .map(|path| path.to_string_lossy().into_owned())
            })
        })
        .collect::<Vec<_>>();

    let mut prost_config = prost_build::Config::default();

    // This is needed to convert enum variant -> string in order to get the action name in rust
    prost_config.type_attribute("Action.data", "#[derive(strum_macros::Display)]");

    let out_dir = std::env::var("OUT_DIR")?;
    tonic_build::configure()
        .file_descriptor_set_path(std::path::Path::new(&out_dir).join("descriptor.bin"))
        .compile_with_config(prost_config, &source_proto_files, &includes)?;

    // Compile Google Cloud protos for inlined gcloud module
    tonic_build::configure().build_server(false).compile(
        &[
            proto_root.join("google/api/annotations.proto"),
            proto_root.join("google/api/client.proto"),
            proto_root.join("google/api/field_behavior.proto"),
            proto_root.join("google/api/http.proto"),
            proto_root.join("google/api/resource.proto"),
            proto_root.join("google/iam/v1/iam_policy.proto"),
            proto_root.join("google/iam/v1/options.proto"),
            proto_root.join("google/iam/v1/policy.proto"),
            proto_root.join("google/longrunning/operations.proto"),
            proto_root.join("google/rpc/status.proto"),
            proto_root.join("google/type/expr.proto"),
            proto_root.join("google/pubsub/v1/pubsub.proto"),
            proto_root.join("google/cloud/kms/v1/service.proto"),
            proto_root.join("google/cloud/kms/v1/resources.proto"),
            proto_root.join("google/crypto/tink/aes_gcm.proto"),
        ],
        &[proto_root],
    )?;

    Ok(())
}
