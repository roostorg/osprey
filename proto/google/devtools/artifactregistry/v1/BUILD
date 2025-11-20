load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "artifactregistry_proto",
    srcs = [
        "apt_artifact.proto",
        "artifact.proto",
        "attachment.proto",
        "file.proto",
        "generic.proto",
        "package.proto",
        "repository.proto",
        "rule.proto",
        "service.proto",
        "settings.proto",
        "tag.proto",
        "version.proto",
        "vpcsc_config.proto",
        "yum_artifact.proto",
    ],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google:com_google_protobuf",
        "//proto/google:google_rpc",
        "//proto/google:googleapis",
        "//proto/google/iam/v1:iam_policy_proto",
        "//proto/google/iam/v1:policy_proto",
        "//proto/google/longrunning:operations_proto",
        "//proto/google/type:expr_proto",
    ],
)
