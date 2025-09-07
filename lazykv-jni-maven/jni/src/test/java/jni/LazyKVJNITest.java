package jni;

import org.junit.jupiter.api.Test;

public class LazyKVJNITest {
  @Test
  void simpleTest() {
    System.out.println("Start testing!");
  
    String bePropPath = "/sharedfs/LazyLog-Artifact/cfg_datalog/be.prop";
    String dlClientPath = "/sharedfs/LazyLog-Artifact/cfg_datalog/dl_client.prop";
    String rdmaPath = "/sharedfs/LazyLog-Artifact/cfg_datalog/rdma.prop";
    java.util.concurrent.atomic.AtomicLong startTime = new java.util.concurrent.atomic.AtomicLong();
    Thread[] threads = new Thread[6];
    for (int i = 0; i < 3; i++) {
      final int index = i;
      threads[i] = new Thread(() -> {
        LazyKVJNI kvWriter = new LazyKVJNI();
        kvWriter.init(bePropPath, dlClientPath, rdmaPath);
        for (int j = 0; j < 10000; j++) {
          String key = index + "key" + j;
          kvWriter.insert(key, key.getBytes());
        }
      });
      threads[i + 3] = new Thread(() -> {
        LazyKVJNI kvReader = new LazyKVJNI();
        kvReader.init(bePropPath, dlClientPath, rdmaPath);
        // sleep for 10 seconds
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          System.err.println("Reader thread interrupted: " + e.getMessage());
        }
        for (int j = 0; j < 10000; j++) {
          String key = index + "key" + j;
          byte[] value = kvReader.read(key);
        }
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
    long endTime = System.currentTimeMillis();
    long elapsedTime = endTime - startTime.get();
    System.out.println("Elapsed time: " + elapsedTime*1.0/1000 + " s");
    System.out.println("Successfully cleaned up!");
  }
}