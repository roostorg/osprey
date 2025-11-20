# !!!
# YOU ALMOST CERTAINLY DO NOT WANT TO ADD LIBRARIES TO THIS FILE!
# This directory mirrors the Google official protobuf repos, e.g:
# * https://github.com/protocolbuffers/protobuf/tree/main/src/google
# * https://github.com/googleapis/googleapis/tree/master/google
# The BAZEL file structure used in those repos should be used in this local
# copy (directory) as well, i.e. libraries should be added to BUILD files
# within the directory that contains the `.proto`s themselves
# (and not at the top-level directory).
# !!!

load("@rules_proto//proto:defs.bzl", "proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "com_google_protobuf",
    srcs = glob(["protobuf/**/*.proto"]),
    strip_import_prefix = "/proto/",
)

filegroup(
    name = "google_protobuf_files",
    srcs = glob(["protobuf/**/*.proto"]),
)

proto_library(
    name = "googleapis",
    srcs = glob(["api/**/*.proto"]),
    strip_import_prefix = "/proto/",
    deps = [
        ":com_google_protobuf",
    ],
)

proto_library(
    name = "google_rpc",
    srcs = glob(["rpc/**/*.proto"]),
    strip_import_prefix = "/proto/",
    deps = [
        ":com_google_protobuf",
    ],
)

java_proto_library(
    name = "com_google_protobuf_java",
    visibility = ["//visibility:public"],
    deps = [":com_google_protobuf"],
)
