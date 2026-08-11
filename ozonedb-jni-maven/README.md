# ozonedb-jni-maven

Only `corfu-bridge/` remains here. It is a standalone Maven module — it has no parent pom — that
produces `target/corfu-bridge-1.0-all.jar`, the Java shim `CorfuDBStorage` loads into its embedded
JVM. CMake builds it as a side effect of `-DOZONEDB_ENABLE_CORFU=ON`; you should not need to run
Maven here by hand.

It must be built by a JDK new enough for the CorfuDB runtime (currently 25) and compiles to a
Java 11 target, which is *not* the JDK the rest of the chain uses. See `CLAUDE.md`.

The directory name is legacy. Three other modules used to live here:

| Was | Did | Now |
|---|---|---|
| `pom.xml` | parent; a `system`-scoped dependency on `libOzoneDB.so` used to order the build | gone |
| `native/` | built `libozonedb.so` with `native-maven-plugin` | CMake target `ozonedb_jni`, sources in `src/jni/` |
| `jni/` | shipped a 10-line `jni.OzoneDBJNI` as a fat jar installed into `~/.m2` | a source file in the YCSB binding, `ycsb/ozonedb/src/main/java/jni/` |
