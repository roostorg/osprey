load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "pubsub_proto",
    srcs = [
        "pubsub.proto",
        "schema.proto",
    ],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google:com_google_protobuf",
        "//proto/google:googleapis",
    ],
)
