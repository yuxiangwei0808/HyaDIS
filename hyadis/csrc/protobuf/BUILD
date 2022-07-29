load("@rules_proto//proto:defs.bzl", "proto_library")
load("@com_github_grpc_grpc//bazel:cc_grpc_library.bzl", "cc_grpc_library")
load("@com_github_grpc_grpc//bazel:grpc_build_system.bzl", "grpc_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "scheduler_proto",
    srcs = ["scheduler.proto"],
    deps = [
        "@com_github_ray_project_ray//src/ray/protobuf:node_manager_proto",
    ],
)

cc_proto_library(
    name = "scheduler_cc_proto",
    deps = [":scheduler_proto"],
)

cc_grpc_library(
    name = "scheduler_cc_grpc",
    srcs = [":scheduler_proto"],
    grpc_only = True,
    deps = [":scheduler_cc_proto"],
)
