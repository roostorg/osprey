load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "iam_policy_proto",
    srcs = ["iam_policy.proto"],
    strip_import_prefix = "/proto/",
    deps = [
        ":options_proto",
        ":policy_proto",
        "//proto/google:com_google_protobuf",
        "//proto/google:googleapis",
    ],
)

proto_library(
    name = "policy_proto",
    srcs = ["policy.proto"],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google/type:expr_proto",
    ],
)

proto_library(
    name = "options_proto",
    srcs = ["options.proto"],
    strip_import_prefix = "/proto/",
)
