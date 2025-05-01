package jni;

import java.util.*;

public class LazyKVJNI {
  static {
    System.loadLibrary("lazykv");
  }

  public native void init(String clientUri, String writeServerUri, String readServerUri, int phyPort, int msgSize);
  public native int insert(String table, String key, Map<String, String> values);
  public native int read(String table, String key, Map<String, String> result);
  public native void cleanup();
}
