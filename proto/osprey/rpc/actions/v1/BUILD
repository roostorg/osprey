load("@rules_java//java:defs.bzl", "java_proto_library")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "actions_proto",
    srcs = [
        "action.proto",
        "action_types.proto",
        "application.proto",
        "captcha.proto",
        "channel.proto",
        "guild.proto",
        "invite.proto",
        "metadata.proto",
        "user.proto",
    ],
    strip_import_prefix = "/proto",
    visibility = ["//visibility:public"],
    deps = [
        "//proto/discord_protos/discord_authentication/v1:session_proto",
        "//proto/discord_protos/users/v1:user_proto",
        "//proto/google:com_google_protobuf",
    ],
)

java_proto_library(
    name = "actions_proto_java",
    visibility = ["//visibility:public"],
    deps = [":actions_proto"],
)
