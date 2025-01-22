package jni;
public class OzoneDBJNI {
    static {
        System.loadLibrary("ozonedb");
    }

    // Native method declaration
    public native void openDB(String configPath);
    public native void closeDB();
    public native void put(String key, byte[] value);
    public native byte[] get(String key);
    public native void remove(String key);
}