cmake_minimum_required(VERSION 3.10)

project(lazylog_cli_project)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Include headers
include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(/sharedfs/LazyLog-Artifact/src)
include_directories(/sharedfs/LazyLog-Artifact/HdrHistogram_c/include)

link_directories(${CMAKE_SOURCE_DIR}/lib)

add_executable(append_bench append_bench.cc)
add_executable(basic_client basic_client.cc)

target_link_libraries(append_bench lazylogcli backendcli hdr_histogram ibverbs)
target_link_libraries(basic_client lazylogcli backendcli ibverbs)

target_compile_definitions(append_bench PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(basic_client PRIVATE ERPC_INFINIBAND=true)
