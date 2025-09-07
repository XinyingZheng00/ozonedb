package jni;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class OzoneDBJNITest {

    @Test
    void simpleTest() {
        OzoneDBJNI db = new OzoneDBJNI();
        String ozonedbHome = System.getenv("OZONEDB_HOME");
        int threadNum = 4;
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Thread(() -> {
                db.openDB(ozonedbHome + "/src/config/local/shared_config_rocksdb_base.json");
                for (int j = 0; j < 100; j++) {
                    String key = "key" + Thread.currentThread().getName();
                    String value = "value" + Thread.currentThread().getName();
                    db.put(key, value.getBytes());
                }
                for (int j = 0; j < 100; j++) {
                    String key = "key" + Thread.currentThread().getName();
                    byte[] value = db.get(key);
                    String valueStr = new String(value);
                    assertEquals(valueStr, ("value" + Thread.currentThread().getName()));
                }
                // db.closeDB();
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Main thread interrupted while waiting for writer/reader: " + e.getMessage());
        }
    }
}
