cmake_minimum_required(VERSION 3.10)

project(lazylog_cli_project)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT ERPC_DIR)
    set(ERPC_DIR /sharedfs/eRPC)
endif()
message("ERPC dir is ${ERPC_DIR}")
find_package(Glog REQUIRED)

# Include headers
include_directories(/usr/local/include)
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(/sharedfs/LazyLog-Artifact/src)
include_directories(/sharedfs/LazyLog-Artifact/HdrHistogram_c/include)
include_directories(
    ${ERPC_DIR}/src
    ${ERPC_DIR}/third_party/asio/include
)
link_directories(${CMAKE_SOURCE_DIR}/lib)

add_executable(append_bench append_bench.cc)
add_executable(basic_client basic_client.cc)

target_link_libraries(append_bench lazylogcli backendcli hdr_histogram ibverbs glog::glog erpc numa)
target_link_libraries(basic_client lazylogcli backendcli ibverbs glog::glog erpc numa)

target_compile_definitions(append_bench PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(basic_client PRIVATE ERPC_INFINIBAND=true)
