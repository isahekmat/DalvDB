package(default_visibility = ["//visibility:public"])

load("@rules_proto_grpc//cpp:defs.bzl", "cpp_proto_compile")
load("@rules_proto_grpc//cpp:defs.bzl", "cpp_proto_library")
load("@rules_proto_grpc//cpp:defs.bzl", "cpp_grpc_compile")

load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
    name = "common_proto",
    srcs = ["common.proto"],
)

proto_library(
    name = "user_service_proto",
    srcs = ["user_service.proto"],
    deps = [":common_proto"],
)

cpp_proto_library(
    name = "common_cpp_proto",
    protos = [":common_proto"],
)

cpp_grpc_compile(
    name = "user_service",
    protos = [":user_service_proto"],
)
