load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "resourcemanager_proto",
    srcs = [
        "folders.proto",
        "organizations.proto",
        "projects.proto",
        "tag_holds.proto",
        "tag_keys.proto",
        "tag_values.proto",
    ],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google:com_google_protobuf",
        "//proto/google:googleapis",
        "//proto/google/iam/v1:iam_policy_proto",
        "//proto/google/iam/v1:policy_proto",
        "//proto/google/longrunning:operations_proto",
    ],
)
