# Protobuf headers are generated into build/generated/ by CMake now, so there
# is no in-tree protobuf directory to create or clear here.
mkdir -p $OZONEDB_HOME/build
cd $OZONEDB_HOME/build
cmake -DOZONEDB_ENABLE_CORFU=ON .. && make -j20 OzoneDB corfu-bridge
mkdir -p ../ozonedb-jni-maven/native/src/main/cpp/lib
cp libOzoneDB.so ../ozonedb-jni-maven/native/src/main/cpp/lib/.
cd ../ozonedb-jni-maven
sudo mkdir -p /tank && sudo chmod 777 /tank && mvn clean package -DskipTests
sudo cp ${OZONEDB_HOME}/ozonedb-jni-maven/jni/target/classes/libozonedb.so /usr/lib/
sudo cp ${OZONEDB_HOME}/build/libOzoneDB.so /usr/lib/
mvn install:install-file -Dfile=${OZONEDB_HOME}/ozonedb-jni-maven/jni/target/demoproc-jni-1.0-jar-with-dependencies.jar -DgroupId=ozonedbjnimaven -DartifactId=demoproc-jni -Dversion=1.0 -Dpackaging=jar
cd $OZONEDB_HOME/bench/scripts
