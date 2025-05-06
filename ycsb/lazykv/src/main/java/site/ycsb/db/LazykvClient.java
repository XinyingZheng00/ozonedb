package site.ycsb.db;

import java.util.*;
import jni.LazyKVJNI;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * LazyKV client for YCSB framework.
 */
public class LazykvClient extends DB {
  private static final String PROP_LLKV_BE_PATH = "be.path";
  private static final String PROP_LLKV_BE_PATH_DEFAULT = "/sharedfs/LazyLog-Artifact/cfg_datalog/be.prop";

  private static final String PROP_LLKV_DL_CLIENT_PATH = "dl_client.path";
  private static final String PROP_LLKV_DL_CLIENT_PATH_DEFAULT = "/sharedfs/LazyLog-Artifact/cfg_datalog/" + 
      "dl_client.prop";

  private static final String PROP_LLKV_RDMA_PATH = "rdma.path";
  private static final String PROP_LLKV_RDMA_PATH_DEFAULT = "/sharedfs/LazyLog-Artifact/cfg_datalog/rdma.prop";

  private LazyKVJNI kv = null;

  @Override
  public void init() throws DBException {
    if (kv == null){
      String bePath = getProperty(PROP_LLKV_BE_PATH, PROP_LLKV_BE_PATH_DEFAULT);
      String dlClientPath = getProperty(PROP_LLKV_DL_CLIENT_PATH, PROP_LLKV_DL_CLIENT_PATH_DEFAULT);
      String rdmaPath = getProperty(PROP_LLKV_RDMA_PATH, PROP_LLKV_RDMA_PATH_DEFAULT);
      kv = new LazyKVJNI();
      kv.init(bePath, dlClientPath, rdmaPath);
    }
  }

  private String getProperty(String name, String def) {
    return getProperties().getProperty(name, def);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    byte[] value = null;
    try {
      value = serializeValues(values);
    } catch (IOException e) {
      e.printStackTrace();
    }
    kv.insert(key, value);
    return Status.OK;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[] values = kv.read(key);
    if (values == null) {
      return Status.ERROR;
    }
    deserializeValues(values, fields, result);
    int totalBytes = key.length() + values.length;
    return Status.OK;
  }

  @Override
  public void cleanup() throws DBException {
    kv.cleanup();
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result) {
    // NOT SUPPORTED
    System.out.println("scan is not supported");
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    final Map<String, ByteIterator> result = new HashMap<>();
    byte[] currentValue = kv.read(key);
    deserializeValues(currentValue, null, result);

    result.putAll(values);

    byte[] newValue = null;
    int totalBytes = 0;
    try {
      newValue = serializeValues(result);
      kv.insert(key, newValue);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    // NOT SUPPORTED
    System.out.println("delete is not supported");
    return Status.OK;
  }


  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);
      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(StandardCharsets.UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);
    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }
    return result;
  }
}
