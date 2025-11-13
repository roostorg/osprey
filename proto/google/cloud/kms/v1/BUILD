load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "kms_proto",
    srcs = [
        "resources.proto",
        "service.proto",
    ],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google:com_google_protobuf",
        "//proto/google:googleapis",
    ],
)
