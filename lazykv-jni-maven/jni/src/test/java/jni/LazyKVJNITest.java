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
    //sleep 20 seconds
    // try {
    //   // Thread.sleep(30000);
    // } catch (InterruptedException e) {
    //   Thread.currentThread().interrupt();
    //   System.err.println("Main thread interrupted while sleeping: " + e.getMessage());
    // }
    Thread[] threads = new Thread[2];
    int cores = Runtime.getRuntime().availableProcessors();
    System.out.println("Number of cores: " + cores);
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread(() -> {
        LazyKVJNI kvWriter = new LazyKVJNI();
        kvWriter.init(bePropPath, dlClientPath, rdmaPath);
        startTime.set(System.currentTimeMillis());
        for (int j = 0; j < 100; j++) {
          String key = "key" + j;
          //have a 1kB value
          char[] chars = new char[1024]; // 1KB value
          java.util.Arrays.fill(chars, 'a');
          String value = new String(chars);
          kvWriter.insert(key, value.getBytes());
        }
        // kvWriter.cleanup();
      });
    }
    //start time
    System.out.println("Start time: " + startTime.get());
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
    //end time
    long endTime = System.currentTimeMillis();
    long elapsedTime = endTime - startTime.get();
    System.out.println("Elapsed time: " + elapsedTime*1.0/1000 + " s");
    System.out.println("Successfully cleaned up!");
  }
}