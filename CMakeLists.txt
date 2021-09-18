cmake_minimum_required(VERSION 3.5.1)

set(PACKAGE_NAME "gfs")
project(${PACKAGE_NAME} CXX)

add_compile_options(-std=c++11 -Wall -Werror -pedantic -g)

list(APPEND CMAKE_PREFIX_PATH "/opt/grpc" "/opt/protobuf")

find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)

# Proto file
get_filename_component(gfs_proto "protos/gfs.proto" ABSOLUTE)
get_filename_component(gfs_proto_path "${gfs_proto}" PATH)

# Generated sources
set(gfs_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/gfs.pb.cc")
set(gfs_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gfs.pb.h")
set(gfs_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/gfs.grpc.pb.cc")
set(gfs_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/gfs.grpc.pb.h")


add_custom_command(
      OUTPUT "${gfs_proto_srcs}" "${gfs_proto_hdrs}" "${gfs_grpc_srcs}" "${gfs_grpc_hdrs}"
      COMMAND protoc
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${gfs_proto_path}"
        --plugin=protoc-gen-grpc=${_GRPC_CPP_PLUGIN_EXECUTABLE}
        "${gfs_proto}"
      DEPENDS "${gfs_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# gfs_grpc_proto
add_library(gfs_grpc_proto
  ${gfs_grpc_srcs}
  ${gfs_grpc_hdrs}
  ${gfs_proto_srcs}
  ${gfs_proto_hdrs})

target_link_libraries(gfs_grpc_proto
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Targets: master cserver
foreach(_target master cserver client)
  add_executable(${_target} "src/${_target}.cpp")
  target_link_libraries(${_target}
    gfs_grpc_proto
    gRPC::grpc++_reflection
    protobuf::libprotobuf
  )
  target_include_directories(${_target} PRIVATE "include")
endforeach()
