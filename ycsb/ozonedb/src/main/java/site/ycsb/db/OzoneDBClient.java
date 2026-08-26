package site.ycsb.db;

import site.ycsb.DB;
import site.ycsb.Status;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import java.util.Set;
import java.util.Map;
import java.util.Vector;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import site.ycsb.DBException;
import jni.OzoneDBJNI;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OzoneDB client for YCSB framework.
 */
public class OzoneDBClient extends DB {
  private static final String CONFIG_PROPERTY = "shared_config";
  private static final String OZONEDB_HOME = System.getenv("OZONEDB_HOME");
  private static final String CONFIG_DEFAULT = OZONEDB_HOME + "/src/shared_config_rocksdb.json";

  private static final Logger LOGGER = LoggerFactory.getLogger(OzoneDBClient.class);
  private OzoneDBJNI db = null;
  private long lastTime = 0;

  @Override
  public void init() throws DBException {
    if (db == null) {
      LOGGER.info("Initializing OzoneDB client with shared config: " + CONFIG_DEFAULT);
      db = new OzoneDBJNI();
      final Properties props = getProperties();
      String sharedConfig = props.getProperty(CONFIG_PROPERTY, CONFIG_DEFAULT);
      db.openDB(sharedConfig);
      lastTime = System.currentTimeMillis();
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    byte[] values = db.get(key);
    if (values == null) {
      return Status.NOT_FOUND;
    }
    deserializeValues(values, fields, result);
    int totalBytes = key.length() + values.length;
    reportThroughput(totalBytes); // Report throughput after each read
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.info("scan is not supported");
    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    // Read-modify-write. A missing current value is NOT a reason to skip
    // the write: in a multi-writer cell the key often belongs to a peer
    // whose record is not visible here yet. Returning NOT_FOUND dropped
    // the update entirely and still counted a fast completed operation,
    // so throughput read high for work that never happened.
    final Map<String, ByteIterator> result = new HashMap<>();
    byte[] currentValue = db.get(key);
    if (currentValue != null) {
      deserializeValues(currentValue, null, result);
    }

    // update
    result.putAll(values);

    byte[] newValue;
    try {
      newValue = serializeValues(result);
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
    if (!db.put(key, newValue)) {
      return Status.ERROR;
    }
    reportThroughput(key.length() + newValue.length);
    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    // LOGGER.info(values.toString()); don't print values using logger, due to some
    // mysterial reason, the size of value will be changed.
    byte[] value = null;
    try {
      value = serializeValues(values);
    } catch (Exception e) {
      e.printStackTrace();
    }
    db.put(key, value);
    int totalBytes = key.getBytes(StandardCharsets.UTF_8).length + value.length;
    reportThroughput(totalBytes); // Report throughput after each insert
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    db.remove(key);
    return Status.OK;
  }

  @Override
  public void cleanup() {
    db.closeDB();
  }

  // Method to report throughput
  private void reportThroughput(long size) {
    // long currentTime = System.currentTimeMillis();
    // long elapsedTime = currentTime - lastTime; // Elapsed time in nanoTime
    // lastTime = currentTime;
    // if (elapsedTime > 0) {
    //   // LOGGER.info("Current size: " + size);
    //   // LOGGER.info("Current elapsed time: " + elapsedTime);
    //   double throughput = size / 1024.0 / 1024.0 / (elapsedTime * 1.0 / 1000.0); // MB per second
    //   LOGGER.info("Current Throughput: " + throughput + " MB/sec at " + currentTime);
    // }
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
