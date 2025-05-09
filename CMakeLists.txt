# set(CMAKE_BUILD_TYPE DEBUG) # enable debug symbols
# link_directories($ENV{OZONEDB_HOME}/build/vcpkg_installed/x64-linux/lib)
# include_directories(/usr/local/include)

cmake_minimum_required(VERSION 3.16)

# Toolchain and Prefix Path
set(CMAKE_TOOLCHAIN_FILE "$ENV{OZONEDB_HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake")

# Project Configuration
project(OzoneDB VERSION 1.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_BUILD_TYPE DEBUG)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

if(NOT ERPC_DIR)
    set(ERPC_DIR /sharedfs/eRPC)
endif()
if (NOT LAZYLOG_DIR)
    set(LAZYLOG_DIR /sharedfs/LazyLog-Artifact)
endif()
message("ERPC dir is ${ERPC_DIR}")
message("LazyLog dir is ${LAZYLOG_DIR}")

# Find Packages
link_directories(${PROJECT_SOURCE_DIR}/lazylog/lib)
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(unofficial-sqlite3 CONFIG REQUIRED)
find_package(Boost 1.70 REQUIRED COMPONENTS uuid)
find_package(OpenSSL CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(log4cxx CONFIG REQUIRED)
find_package(Glog REQUIRED)

# Protobuf Compilation
protobuf_generate_cpp(
    PROTO_SRCS PROTO_HDRS 
    ${PROJECT_SOURCE_DIR}/src/include/ozonedb/record.proto 
    ${PROJECT_SOURCE_DIR}/src/include/ozonedb/sstable.proto 
    PROTOC_OUT_DIR ${PROJECT_SOURCE_DIR}/src/include/ozonedb/protobuf
)

# Include Directories
include_directories(${PROJECT_SOURCE_DIR}/lazylog/include)
include_directories(/sharedfs/LazyLog-Artifact/src)
include_directories(/sharedfs/LazyLog-Artifact/HdrHistogram_c/include)
include_directories(
    ${ERPC_DIR}/src
    ${ERPC_DIR}/third_party/asio/include
)
include_directories(
    ${GTEST_INCLUDE_DIRS}
    ${SQLITE3_INCLUDE_DIRS}
    ${Boost_INCLUDE_DIRS}
    ${PROJECT_SOURCE_DIR}/src/include/ozonedb 
    ${PROJECT_SOURCE_DIR}/src/util/
)

# Library Target
add_library(OzoneDB STATIC
    src/db/compaction.cpp
    src/db/db.cpp
    src/db/log_handler.cpp
    src/db/cache.cpp
    src/db/metadata_log_handler.cpp
    src/db/task_log_handler.cpp
    src/db/storage.cpp
    src/db/azure_blob_storage.cpp
    src/db/shared_log_storage.cpp
    src/db/sstable/block_handler.cpp
    src/db/sstable/table_builder.cpp
    src/db/sstable/iterator.cpp
    src/db/sstable/table_reader.cpp
    src/db/sstable/sstable_handler.cpp
    src/db/sstable/filter_block.cpp
    src/db/sstable/comparator.cpp
    src/db/sstable/bloom.cpp
    src/util/read_json.cpp
    src/util/protobuf_serializer.cpp
    src/util/helper.cpp
    src/util/histogram.cpp
    src/util/test_tool.cpp
    ${PROTO_SRCS} ${PROTO_HDRS}
)

target_link_libraries(OzoneDB
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite 
    ${Boost_LIBRARIES} 
    Azure::azure-identity Azure::azure-storage-blobs 
    OpenSSL::SSL OpenSSL::Crypto
    lazylogcli backendcli hdr_histogram ibverbs glog::glog erpc numa
) # the order of the libraries matters

if(SHARED_LOG)
    add_compile_definitions(SHARED_LOG)
endif()

# Executable for Unit Tests
add_executable(runUnitTests 
    tests/test_db.cpp 
    tests/test_log.cpp
    tests/test_storage.cpp
    tests/test_shared_log.cpp
    tests/test_all.cpp
    tests/test_protobuf.cpp
    tests/test_sstable.cpp
    tests/test_compaction.cpp
    tests/test_filter_block.cpp
)
target_compile_definitions(OzoneDB PRIVATE ERPC_INFINIBAND=true)
target_compile_definitions(runUnitTests PRIVATE ERPC_INFINIBAND=true)

target_link_libraries(runUnitTests 
    GTest::gtest GTest::gtest_main GTest::gmock GTest::gmock_main 
    pthread OzoneDB erpc ibverbs
)

# Custom Clean Targets
add_custom_target(clean-all
    COMMAND ${CMAKE_BUILD_TOOL} clean
    COMMAND rm -rf /tank/db/
)
add_custom_target(clean-db
    COMMAND rm -rf /tank/db/ozonedb
)

# Enable Testing
enable_testing()
