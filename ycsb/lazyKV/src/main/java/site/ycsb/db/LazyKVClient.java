package site.ycsb.db;

import java.util.*;
import jni.LazyKVJNI;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * LazyKV client for YCSB framework
 */
public class LazyKVClient extends DB {
  private static final String PROP_LLKV_WR_SVR_URI = "kv_wr.server_uri";
  private static final String PROP_LLKV_WR_SVR_URI_DEFAULT = "localhost:31870";

  private static final String PROP_LLKV_RD_SVR_URI = "kv_rd.server_uri";
  private static final String PROP_LLKV_RD_SVR_URI_DEFAULT = "localhost:31870";

  private static final String PROP_LLKV_CLI_URI = "kv.client_uri";
  private static final String PROP_LLKV_CLI_URI_DEFAULT = "localhost:31861";

  private static final String PROP_PORT_NUM = "erpc.phy_port";
  private static final String PROP_PORT_NUM_DEFAULT = "0";

  private static final String PROP_MSG_SIZE = "msg.size";
  private static final String PROP_MSG_SIZE_DEFAULT = "8192";

  private LazyKVJNI kv = null;

  @Override
  public void init() throws DBException {
    if (kv == null){
      String clientUri = getProperty(PROP_LLKV_CLI_URI, PROP_LLKV_CLI_URI_DEFAULT);
      String writeUri = getProperty(PROP_LLKV_WR_SVR_URI, PROP_LLKV_WR_SVR_URI_DEFAULT);
      String readUri = getProperty(PROP_LLKV_RD_SVR_URI, PROP_LLKV_RD_SVR_URI_DEFAULT);
      int port = Integer.parseInt(getProperty(PROP_PORT_NUM, PROP_PORT_NUM_DEFAULT));
      int msgSize = Integer.parseInt(getProperty(PROP_MSG_SIZE, PROP_MSG_SIZE_DEFAULT));
      kv = new LazyKVJNI();
      kv.init(clientUri, writeUri, readUri, port, msgSize);
    }
  }

  private String getProperty(String name, String def) {
    return getProperties().getProperty(name, def);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    Map<String, String> stringMap = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      stringMap.put(entry.getKey(), entry.getValue().toString());
    }
    int ret = kv.insert(table, key, stringMap);
    return ret == 0 ? Status.OK : Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    Map<String, String> tmp = new HashMap<>();
    int ret = kv.read(table, key, tmp);
    if (ret != 0) {
      return Status.ERROR;
    }
    for (Map.Entry<String, String> entry : tmp.entrySet()) {
      if (fields.contains(entry.getKey())){
        result.put(entry.getKey(), new StringByteIterator(entry.getValue()));
      }
    }
    return Status.OK;
  }

  @Override
  public void cleanup() throws DBException {
    kv.cleanup();
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // TODO
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // TODO
  }

  @Override
  public Status delete(String table, String key) {
    // TODO
  }
}
