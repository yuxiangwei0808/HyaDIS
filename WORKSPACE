load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "com_github_ray_project_ray",
    remote = "https://github.com/ray-project/ray.git",
    commit = "e4ce38d001dbbe09cd21c497fedd03d692b2be3e",
)

load("@com_github_ray_project_ray//bazel:ray_deps_setup.bzl", "ray_deps_setup")

ray_deps_setup()

load("@com_github_ray_project_ray//bazel:ray_deps_build_all.bzl", "ray_deps_build_all")

ray_deps_build_all()

# This needs to be run after grpc_deps() in ray_deps_build_all() to make
# sure all the packages loaded by grpc_deps() are available. However a
# load() statement cannot be in a function so we put it here.
load("@com_github_grpc_grpc//bazel:grpc_extra_deps.bzl", "grpc_extra_deps")

grpc_extra_deps()

load("@bazel_skylib//lib:versions.bzl", "versions")

# When the bazel version is updated, make sure to update it
# in setup.py as well.
versions.check(minimum_bazel_version = "4.2.1")
