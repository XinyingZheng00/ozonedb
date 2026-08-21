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
   * Batch fence for strict reads (linearizable_reads): sync() takes one
   * storage fence on the calling thread; every get() until clearSync()
   * reuses it, linearizing each read at the sync() point (the cr-sqlite
   * per-tick /barrier analog). Always pair with clearSync(). No-ops in
   * non-strict mode / on non-fencing backends.
   */
  public native void sync();
  public native void clearSync();
}
