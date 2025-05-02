package jni;

import org.junit.jupiter.api.Test;

public class LazyKVJNITest {
  @Test
  void simpleTest() {
    System.out.println("Start testing!");
  

    String bePropPath = "/sharedfs/LazyLog-Artifact/cfg_datalog/be.prop";
    String dlClientPath = "/sharedfs/LazyLog-Artifact/cfg_datalog/dl_client.prop";
    String rdmaPath = "/sharedfs/LazyLog-Artifact/cfg_datalog/rdma.prop";

    // Writer thread
    Thread writerThread = new Thread(() -> {
      LazyKVJNI kvWriter = new LazyKVJNI();
      kvWriter.init(bePropPath, dlClientPath, rdmaPath, 1); // RW: client_id = 1
      System.out.println("WriterReader successfully initialized!");

      for (int i = 0; i < 100; i++) {
        String key = "key" + i;
        String value = "value" + i;
        kvWriter.insert(key, value.getBytes());
      }
      try {
        Thread.sleep(5000); // Wait for some keys to be inserted
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        System.err.println("Reader thread interrupted during initial sleep: " + e.getMessage());
      }

      for (int i = 0; i < 100; i++) {
        String key = "key" + i;
        byte[] value = kvWriter.read(key);
        if (value != null) {
          System.out.println("Read key: " + key + ", value: " + new String(value));
        } else {
          System.out.println("Key not found: " + key);
        }
      }
      System.out.println("Reader cleaned up!");
      kvWriter.cleanup();
    });

    // Reader thread
    Thread readerThread = new Thread(() -> {
      LazyKVJNI kvReader = new LazyKVJNI();
      kvReader.init(bePropPath, dlClientPath, rdmaPath, 0); // Playforward: client_id = 0
      System.out.println("Playforward successfully initialized!");
      kvReader.cleanup();
      System.out.println("Playforward cleaned up!");
    });

    // Start both threads
    writerThread.start();
    readerThread.start();
    // Wait for both threads to complete
    try {
      writerThread.join();
      readerThread.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      System.err.println("Main thread interrupted while waiting for writer/reader: " + e.getMessage());
    }
    System.out.println("Successfully cleaned up!");
  }
}