cmake_minimum_required(VERSION 3.16)

# vcpkg toolchain. Resolved from CMAKE_CURRENT_SOURCE_DIR rather than
# $ENV{OZONEDB_HOME} so the build works from any checkout with nothing
# exported first. Precedence: an explicitly supplied CMAKE_TOOLCHAIN_FILE
# (preset or -D) > $VCPKG_ROOT > the in-repo vcpkg submodule.
if (NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    if (DEFINED ENV{VCPKG_ROOT})
        set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "vcpkg toolchain")
    elseif (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake")
        set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake" CACHE STRING "vcpkg toolchain")
    endif()
endif()

# Project Configuration
project(OzoneDB VERSION 1.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# Profiling build: `cmake -DOZONEDB_PROFILING=ON ..` adds frame pointers
# and line-table debug info so perf/flamegraph produces readable stacks
# without dropping Release optimizations.
option(OZONEDB_PROFILING "Build with frame pointers + debug info for perf/flamegraph" OFF)
if (OZONEDB_PROFILING)
    add_compile_options(-fno-omit-frame-pointer -g)
    add_link_options(-rdynamic)
endif()
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
# set(CMAKE_BUILD_TYPE DEBUG) # enable debug symbols

# Optional backends
option(OZONEDB_ENABLE_CORFU "Build the CorfuDB storage backend (requires JDK + Maven)" OFF)
option(OZONEDB_ENABLE_S3 "Build the S3/MinIO/R2 storage backend (requires aws-sdk-cpp)" ON)

# libozonedb.so, the shim the YCSB binding's jni.OzoneDBJNI loads. Previously
# built by a Maven module (ozonedb-jni-maven/native) via native-maven-plugin.
option(OZONEDB_BUILD_JNI "Build libozonedb.so, the JNI shim the YCSB binding loads" ON)

# Find Packages
#
# No link_directories() here: vcpkg's imported targets already carry their
# own link information, and the old call hardcoded a `build/` directory name.
#
# sqlite3, fmt and log4cxx were previously REQUIRED but are not referenced
# anywhere under src/ or tests/ -- they only made a fresh machine fail
# earlier. sqlite3 served the commented-out runBench target; log4cxx served
# one BasicConfigurator::configure() call in the JNI shim, now removed.
find_package(azure-identity-cpp CONFIG REQUIRED)
find_package(azure-storage-blobs-cpp CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(Boost 1.70 REQUIRED COMPONENTS uuid)
find_package(OpenSSL CONFIG REQUIRED)

# Corfu hosts an embedded JVM, so it hard-requires JNI. The shim only needs
# jni.h, so it degrades to a skip rather than failing a build that never
# wanted a JDK in the first place -- the base build did not require one
# before this target existed.
if (OZONEDB_ENABLE_CORFU)
    find_package(JNI REQUIRED)
    find_program(MAVEN_EXECUTABLE mvn REQUIRED)
elseif (OZONEDB_BUILD_JNI)
    find_package(JNI)
    if (NOT JNI_FOUND)
        message(STATUS "JNI not found; skipping the libozonedb.so shim. Pass -DOZONEDB_BUILD_JNI=OFF to silence this.")
        set(OZONEDB_BUILD_JNI OFF)
    endif()
endif()

if (OZONEDB_ENABLE_S3)
    find_package(AWSSDK CONFIG REQUIRED COMPONENTS s3)
endif()

# Protobuf Compilation
#
# Generated into the build tree, not src/include/ozonedb/protobuf/. Sources
# include these as "protobuf/record.pb.h", so PROTO_GEN_ROOT (not the
# protobuf/ subdir itself) is what goes on the include path.
set(PROTO_GEN_ROOT ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${PROTO_GEN_ROOT}/protobuf)

# Remove the pre-move generation directory. It was CMake-generated output
# (gitignored, protoc-written), and because src/include/ozonedb comes ahead
# of PROTO_GEN_ROOT for other headers, a leftover copy there would silently
# shadow the freshly generated one on any machine that has built before.
if (EXISTS ${PROJECT_SOURCE_DIR}/src/include/ozonedb/protobuf)
    message(STATUS "Removing stale in-tree protobuf output: src/include/ozonedb/protobuf")
    file(REMOVE_RECURSE ${PROJECT_SOURCE_DIR}/src/include/ozonedb/protobuf)
endif()
protobuf_generate_cpp(
    PROTO_SRCS PROTO_HDRS
    ${PROJECT_SOURCE_DIR}/src/include/ozonedb/record.proto
    ${PROJECT_SOURCE_DIR}/src/include/ozonedb/sstable.proto
    PROTOC_OUT_DIR ${PROTO_GEN_ROOT}/protobuf
)

# Library Target
set(OZONEDB_SOURCES
    src/db/compaction.cpp
    src/db/db.cpp
    src/db/log_handler.cpp
    src/db/log_key_index.cpp
    src/db/cache.cpp
    src/db/metadata_log_handler.cpp
    src/db/task_log_handler.cpp
    src/db/checkpoint.cpp
    src/db/storage.cpp
    src/db/disk_cache_storage.cpp
    src/db/azure_blob_storage.cpp
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
if (OZONEDB_ENABLE_CORFU)
    list(APPEND OZONEDB_SOURCES src/db/corfu_storage.cpp src/db/log_trimmer.cpp)
endif()
if (OZONEDB_ENABLE_S3)
    list(APPEND OZONEDB_SOURCES src/db/s3_storage.cpp)
endif()

add_library(OzoneDB SHARED ${OZONEDB_SOURCES})

# PUBLIC so every consumer -- runUnitTests, the corfu smoke binaries, and the
# JNI shim -- inherits these instead of restating them. Public headers such as
# db.h include "protobuf/record.pb.h", so PROTO_GEN_ROOT must be public too.
# PROTO_GEN_ROOT deliberately comes first: it must win over any stale
# src/include/ozonedb/protobuf/ left by a pre-move build.
target_include_directories(OzoneDB PUBLIC
    ${PROTO_GEN_ROOT}
    ${PROJECT_SOURCE_DIR}/src/include/ozonedb
    ${PROJECT_SOURCE_DIR}/src/util
    ${Boost_INCLUDE_DIRS}
)

target_link_libraries(OzoneDB
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite
    ${Boost_LIBRARIES}
    Azure::azure-identity Azure::azure-storage-blobs
    OpenSSL::SSL OpenSSL::Crypto
)

if (OZONEDB_ENABLE_S3)
    target_compile_definitions(OzoneDB PUBLIC OZONEDB_ENABLE_S3)
    target_link_libraries(OzoneDB ${AWSSDK_LINK_LIBRARIES})
endif()

if (OZONEDB_ENABLE_CORFU)
    target_compile_definitions(OzoneDB PUBLIC OZONEDB_ENABLE_CORFU)
    target_include_directories(OzoneDB PUBLIC ${JNI_INCLUDE_DIRS})
    target_link_libraries(OzoneDB ${JNI_LIBRARIES})

    set(CORFU_BRIDGE_POM "${PROJECT_SOURCE_DIR}/ozonedb-jni-maven/corfu-bridge/pom.xml")
    # maven-shade-plugin names the *attached* shaded artifact
    # ${artifactId}-${version}-${classifier}.jar and ignores <finalName>, so
    # this is corfu-bridge-1.0-all.jar. Naming it corfu-bridge-all.jar meant
    # the OUTPUT never appeared, so `mvn package` re-ran on every build and
    # every incremental C++ build blocked on Maven. Keep in sync with the
    # <version> in corfu-bridge/pom.xml.
    set(CORFU_BRIDGE_VERSION "1.0")
    set(CORFU_BRIDGE_JAR "${PROJECT_SOURCE_DIR}/ozonedb-jni-maven/corfu-bridge/target/corfu-bridge-${CORFU_BRIDGE_VERSION}-all.jar")
    add_custom_command(
        OUTPUT ${CORFU_BRIDGE_JAR}
        COMMAND ${MAVEN_EXECUTABLE} -f ${CORFU_BRIDGE_POM} package -DskipTests
        DEPENDS ${CORFU_BRIDGE_POM}
                ${PROJECT_SOURCE_DIR}/ozonedb-jni-maven/corfu-bridge/src/main/java/site/ycsb/db/corfu/CorfuBridge.java
        COMMENT "Building corfu-bridge fat jar"
        VERBATIM
    )
    add_custom_target(corfu-bridge ALL DEPENDS ${CORFU_BRIDGE_JAR})
    add_dependencies(OzoneDB corfu-bridge)
endif()

# JNI shim -> libozonedb.so
#
# Replaces the ozonedb-jni-maven/native Maven module, which built this with
# native-maven-plugin 1.0-alpha-8 and a hand-maintained -I/-L list. Linking
# OzoneDB here means include paths and link deps are inherited instead.
#
# The $ORIGIN rpath lets libozonedb.so find libOzoneDB.so sitting beside it in
# the build directory, so neither has to be copied into /usr/lib. OUTPUT_NAME
# differs from the OzoneDB target only by case, which is fine on the Linux
# bench nodes this targets but would collide on a case-insensitive filesystem.
if (OZONEDB_BUILD_JNI)
    add_library(ozonedb_jni SHARED src/jni/jni_OzoneDBJNI.cpp)
    set_target_properties(ozonedb_jni PROPERTIES
        OUTPUT_NAME ozonedb
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "$ORIGIN"
    )
    target_include_directories(ozonedb_jni PRIVATE
        ${JNI_INCLUDE_DIRS}
        ${PROJECT_SOURCE_DIR}/src/jni
    )
    # OzoneDB only -- the shim implements Java_jni_* entry points and uses the
    # JNIEnv it is handed, so it must not link libjvm itself.
    target_link_libraries(ozonedb_jni PRIVATE OzoneDB)
endif()

# Executable for Unit Tests
#
# test_log.cpp and test_compaction.cpp are omitted: both are entirely
# commented out, so they contributed nothing but compile time.
set(OZONEDB_TEST_SOURCES
    tests/test_db.cpp
    tests/test_storage.cpp
    tests/test_all.cpp
    tests/test_protobuf.cpp
    tests/test_sstable.cpp
    tests/test_filter_block.cpp
    tests/test_checkpoint.cpp
    tests/test_cache.cpp
    tests/test_disk_cache_storage.cpp
)
if (OZONEDB_ENABLE_CORFU)
    list(APPEND OZONEDB_TEST_SOURCES tests/test_corfu_storage.cpp)
endif()
add_executable(runUnitTests ${OZONEDB_TEST_SOURCES})
if (OZONEDB_ENABLE_CORFU)
    target_compile_definitions(runUnitTests PRIVATE OZONEDB_ENABLE_CORFU)
    target_include_directories(runUnitTests PRIVATE ${JNI_INCLUDE_DIRS})
    target_link_libraries(runUnitTests ${JNI_LIBRARIES})

    add_executable(corfu_smoke tests/corfu_smoke.cpp)
    target_include_directories(corfu_smoke PRIVATE ${JNI_INCLUDE_DIRS})
    target_link_libraries(corfu_smoke OzoneDB ${JNI_LIBRARIES})

    add_executable(corfu_multiwriter_smoke tests/corfu_multiwriter_smoke.cpp)
    target_include_directories(corfu_multiwriter_smoke PRIVATE ${JNI_INCLUDE_DIRS})
    target_link_libraries(corfu_multiwriter_smoke OzoneDB ${JNI_LIBRARIES})

    # Read-only stream diagnostic (dropped-above-seal appends, metadata.log
    # rollforward simulation). See tests/corfu_stream_stats.cpp.
    add_executable(corfu_stream_stats tests/corfu_stream_stats.cpp)
    target_include_directories(corfu_stream_stats PRIVATE ${JNI_INCLUDE_DIRS})
    target_link_libraries(corfu_stream_stats OzoneDB ${JNI_LIBRARIES})
endif()

target_link_libraries(runUnitTests 
    protobuf::libprotoc protobuf::libprotobuf protobuf::libprotobuf-lite 
    GTest::gtest GTest::gtest_main GTest::gmock GTest::gmock_main 
    pthread OzoneDB
)

# add_executable(runBench 
# bench/bench.cpp
# )
# target_link_libraries(runBench OzoneDB unofficial::sqlite3::sqlite3 rocksdb log4cxx)

# Custom Clean Targets
#
# clean-all used to `rm -rf /tank/db/`, wiping every database under /tank --
# including other users' and other backends'. It now scopes to the same
# ozonedb directory clean-db uses.
add_custom_target(clean-all
    COMMAND ${CMAKE_BUILD_TOOL} clean
    COMMAND rm -rf /tank/db/ozonedb
)
add_custom_target(clean-db
    COMMAND rm -rf /tank/db/ozonedb
)

# Enable Testing
enable_testing()
