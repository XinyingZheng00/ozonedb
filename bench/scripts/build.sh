#!/usr/bin/env bash
#
# Build everything a YCSB run needs: libOzoneDB.so, libozonedb.so (the JNI
# shim), corfu-bridge-1.0-all.jar, and the YCSB ozonedb binding.
#
# Run this after any C++ change that a YCSB run must see. Nothing is installed
# outside the repo: libozonedb.so carries an $ORIGIN rpath so it finds
# libOzoneDB.so beside it in build/, and jni.OzoneDBJNI resolves the shim via
# $OZONEDB_HOME/build. That means no sudo, and no stale copy in /usr/lib
# silently winning over a fresh build.
#
# Was update_jni.sh, which also had to copy two .so files into /usr/lib and
# install a jar into ~/.m2 for three now-deleted Maven modules.

set -euo pipefail

: "${OZONEDB_HOME:?OZONEDB_HOME must be set (see bench/scripts/setup_node.sh)}"

JOBS="$(nproc 2>/dev/null || echo 4)"

# /tank is where the bench configs put db_path.
sudo mkdir -p /tank && sudo chmod 777 /tank

cmake -B "$OZONEDB_HOME/build" -S "$OZONEDB_HOME" -DOZONEDB_ENABLE_CORFU=ON
# Only the targets a YCSB run loads. The default `all` also builds
# runUnitTests, whose sources have drifted from the current Table::get
# signature and fail to compile -- that must not block a bench deploy.
cmake --build "$OZONEDB_HOME/build" -j"$JOBS" --target OzoneDB --target ozonedb_jni

cd "$OZONEDB_HOME/ycsb"
mvn -pl site.ycsb:ozonedb-binding -am package -DskipTests

echo
echo "built:"
echo "  $OZONEDB_HOME/build/libOzoneDB.so"
echo "  $OZONEDB_HOME/build/libozonedb.so"
echo "  $OZONEDB_HOME/ozonedb-jni-maven/corfu-bridge/target/corfu-bridge-1.0-all.jar"
