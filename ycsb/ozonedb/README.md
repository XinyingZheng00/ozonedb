<!--
Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

```
sudo cp ${OZONEDB_HOME}/ozonedb-jni-maven/jni/target/classes/libozonedb.so /usr/lib/
mvn install:install-file -Dfile=/users/Xinying/ozonedb/ozonedb-jni-maven/jni/target/demoproc-jni-1.0-jar-with-dependencies.jar -DgroupId=ozonedbjnimaven -DartifactId=demoproc-jni -Dversion=1.0 -Dpackaging=jar

mvn -pl site.ycsb:ozonedb-binding -am clean package
./bin/ycsb load ozonedb -s -P workloads/workloada (-p property)

```
