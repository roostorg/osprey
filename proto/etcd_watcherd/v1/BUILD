load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "etcd_watcherd_proto",
    srcs = ["etcd_watcherd.proto"],
    strip_import_prefix = "/proto/",
    deps = [
        "//proto/google:com_google_protobuf",
    ],
)
