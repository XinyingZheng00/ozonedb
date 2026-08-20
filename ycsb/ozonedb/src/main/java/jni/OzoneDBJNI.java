package jni;

import java.io.File;

/**
 * JNI binding to libozonedb.so.
 *
 * <p>The package must stay {@code jni}: the exported C symbols in
 * src/jni/jni_OzoneDBJNI.h are javah-generated as {@code Java_jni_OzoneDBJNI_*},
 * so renaming the package silently breaks the lookup at load time.
 */
public class OzoneDBJNI {

  private static final String LIB_PROPERTY = "ozonedb.native.lib";
  private static final String LIB_FILENAME = "libozonedb.so";

  static {
    loadNativeLibrary();
  }

  /**
   * Resolution order, first hit wins:
   *
   * <ol>
   *   <li>{@code -Dozonedb.native.lib=/abs/path/libozonedb.so} — explicit override.
   *   <li>{@code $OZONEDB_HOME/build/libozonedb.so} — where CMake writes it. Its RPATH is
   *       $ORIGIN, so the libOzoneDB.so beside it is found without any install step.
   *   <li>{@code System.loadLibrary} — honours -Djava.library.path and /usr/lib, so hosts
   *       still carrying an older installed copy keep working.
   * </ol>
   *
   * <p>Resolving here rather than at each call site matters because YCSB is launched several
   * ways — the multiproc runners build their own java command, while the single-process
   * scripts and manual runs go through bin/ycsb, which builds its own.
   */
  private static void loadNativeLibrary() {
    String explicit = System.getProperty(LIB_PROPERTY);
    if (explicit != null && !explicit.isEmpty()) {
      System.load(explicit);
      return;
    }

    String home = System.getenv("OZONEDB_HOME");
    if (home != null && !home.isEmpty()) {
      File built = new File(new File(home, "build"), LIB_FILENAME);
      if (built.isFile()) {
        System.load(built.getAbsolutePath());
        return;
      }
    }

    try {
      System.loadLibrary("ozonedb");
    } catch (UnsatisfiedLinkError e) {
      throw new UnsatisfiedLinkError(
          "Could not load " + LIB_FILENAME + ". Build it with"
              + " `cmake --build build` and export OZONEDB_HOME, or point -D" + LIB_PROPERTY
              + " at it explicitly. Underlying error: " + e.getMessage());
    }
  }

  // Native method declaration
  public native void openDB(String configPath);
  public native void closeDB();
  public native void put(String key, byte[] value);
  public native byte[] get(String key);
  public native void remove(String key);

  /**
   * Versioned read for read-modify-write: null when the key is absent, else an
   * 8-byte big-endian version (the key's global log address, -1 = unwritten)
   * followed by the value bytes. The pair comes from one fenced lookup, so the
   * version is safe to feed straight into {@link #casPut}.
   */
  public native byte[] getVersioned(String key);

  /**
   * Conditional put: takes effect only if the key's version at the write's
   * position in the shared log still equals expectedVersion. Returns the new
   * version (&gt;= 0) on success, -2 on a version conflict (re-read and
   * retry), -1 on failure. Corfu backend only.
   */
  public native long casPut(String key, byte[] value, long expectedVersion);
}
