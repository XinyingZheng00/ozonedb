# cd $OZONEDB_HOME/src/include/ozonedb/protobuf && rm -rf *
mkdir -p $OZONEDB_HOME/src/include/ozonedb/protobuf
mkdir -p $OZONEDB_HOME/build
rm -rf $OZONEDB_HOME/build/CMakeCache.txt
cd $OZONEDB_HOME/build
cmake .. && make -j20
mkdir -p ../ozonedb-jni-maven/native/src/main/cpp/lib
cp libOzoneDB.a ../ozonedb-jni-maven/native/src/main/cpp/lib/.
cd ../ozonedb-jni-maven
sudo mkdir -p /tank && sudo chmod 777 /tank && mvn clean package
sudo cp ${OZONEDB_HOME}/ozonedb-jni-maven/jni/target/classes/libozonedb.so /usr/lib/
mvn install:install-file -Dfile=${OZONEDB_HOME}/ozonedb-jni-maven/jni/target/demoproc-jni-1.0-jar-with-dependencies.jar -DgroupId=ozonedbjnimaven -DartifactId=demoproc-jni -Dversion=1.0 -Dpackaging=jar
cd $OZONEDB_HOME/bench/scripts