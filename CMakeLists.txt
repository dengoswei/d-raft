cmake_minimum_required(VERSION 3.8)

project(d-raft VERSION 0.1)

set(CMAKE_CXX_FLAGS " -g -Wall -std=c++11")

set(
    SOURCES
    raft_mem.cc
    raft_state.cc
    raft_config.cc
    raft_disk_catchup.cc
    progress.cc
    replicate.cc
)



FIND_PACKAGE(Protobuf REQUIRED)
include_directories(${PROTOBUF_INCLUDE_DIR})
PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS raft.proto)


include_directories(cutils)
include_directories(${CMAKE_CURRENT_BINARY_DIR})
#add_subdirectory(cutils)

add_library(d-raft STATIC ${SOURCES} ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(d-raft ${PROTOBUF_LIBRARIES} pthread)

