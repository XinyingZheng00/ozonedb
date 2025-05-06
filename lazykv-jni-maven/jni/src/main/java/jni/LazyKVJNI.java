package jni;

public class LazyKVJNI {
  static {
    System.loadLibrary("lazykv");
  }

  public native void init(String be_prop_path, String cl_client_path, String rdma_path);
  public native void insert(String key, byte[] value);
  public native void update(String key, byte[] value);
  public native byte[] read(String key);
  public native void cleanup();
}
