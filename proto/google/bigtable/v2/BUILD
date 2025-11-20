load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "bigtable_proto",
    srcs = [
        "bigtable.proto",
        "data.proto",
    ],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google:com_google_protobuf",
        "//proto/google:google_rpc",
        "//proto/google:googleapis",
    ],
)
