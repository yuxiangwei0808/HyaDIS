load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_library", "cc_test")
load("@com_github_ray_project_ray//bazel:ray.bzl", "COPTS", "PYX_COPTS", "PYX_SRCS", "copy_to_workspace")

package(default_visibility = ["//visibility:public"])

config_setting(
    name = "msvc-cl",
    flag_values = {"@bazel_tools//tools/cpp:compiler": "msvc-cl"},
)

config_setting(
    name = "clang-cl",
    flag_values = {"@bazel_tools//tools/cpp:compiler": "clang-cl"},
)

config_setting(
    name = "opt",
    values = {"compilation_mode": "opt"},
)

# ================================================================

cc_library(
    name = "hyadis_grpc_lib",
    srcs = glob([
        "hyadis/csrc/rpc/*.cc",
    ]),
    hdrs = glob([
        "hyadis/csrc/rpc/*.h",
    ]),
    copts = COPTS,
    deps = [
        "//hyadis/csrc/protobuf:scheduler_cc_grpc",
        "@com_github_ray_project_ray//:ray_common",
        "@com_github_ray_project_ray//:stats_metric",
        "@com_github_ray_project_ray//:grpc_common_lib",
        "@boost//:asio",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_protobuf//:protobuf",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name="hyadis_scheduler_lib",
    srcs = glob(
        [
            "hyadis/csrc/scheduler/**/*.cc",
        ],
        exclude = [
            "hyadis/csrc/scheduler/main.cc",
        ],
    ),
    hdrs = glob(
        [
            "hyadis/csrc/scheduler/**/*.h",
        ],
    ),
    copts = COPTS,
    linkopts = select({
        "@bazel_tools//src/conditions:windows": [
        ],
        "//conditions:default": [
            "-lpthread",
        ],
    }),
    visibility = ["//visibility:public"],
    deps = [
        ":hyadis_grpc_lib",
        "@com_github_ray_project_ray//:grpc_common_lib",
        "@com_github_ray_project_ray//:ray_common",
        "@com_github_ray_project_ray//:stats_lib",
        "@com_github_ray_project_ray//:ray_util",
        "@com_github_ray_project_ray//:gcs_client_lib",
    ]
)

cc_library(
    name = "hyadis_raylet_lib",
    srcs = glob(
        [
            "hyadis/csrc/thirdparty/ray/raylet/**/*.cc",
            "hyadis/csrc/raylet/**/*.cc",
        ],
        exclude = [
            "hyadis/csrc/raylet/**/*_test.cc",
            "hyadis/csrc/raylet/main.cc",
        ],
    ),
    hdrs = glob(
        [
            "hyadis/csrc/thirdparty/ray/raylet/**/*.h",
            "hyadis/csrc/raylet/**/*.h",
        ],
    ),
    copts = COPTS,
    linkopts = select({
        "@bazel_tools//src/conditions:windows": [
        ],
        "//conditions:default": [
            "-lpthread",
        ],
    }),
    visibility = ["//visibility:public"],
    deps = [
        ":hyadis_scheduler_lib",
        "@com_github_ray_project_ray//:raylet_lib",
    ]
)

cc_binary(
    name = "scheduler",
    srcs = ["hyadis/csrc/scheduler/main.cc"],
    copts = COPTS,
    deps = [
        ":hyadis_scheduler_lib",
        "@com_github_ray_project_ray//:stats_lib",
        "@com_github_ray_project_ray//:ray_util",
        "@com_github_ray_project_ray//:ray_common",
        "@com_github_gflags_gflags//:gflags",
    ],
)

cc_binary(
    name = "raylet",
    srcs = ["hyadis/csrc/raylet/main.cc"],
    copts = COPTS,
    deps = [
        ":hyadis_raylet_lib",
        "@com_github_ray_project_ray//:ray_util",
        "@com_github_ray_project_ray//:raylet_lib",
        "@com_github_gflags_gflags//:gflags",
    ],
)
