package jni;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

public class LazyKVJNITest {
  @Test
  void simpleTest() {
    System.out.println("Start testing! ");
    LazyKVJNI kv = new LazyKVJNI();
    /**
     * kv_wr.server_uri=10.10.1.1:31870
     * kv_rd.server_uri=10.10.1.1:31871
     * kv.client_uri=10.10.1.1:31872
     */
    String wr_server_uri = "10.10.1.1:31870";
    String rd_server_uri = "10.10.1.1:31871";
    String client_uri = "10.10.1.1:31872";
    int phy_port = 1;
    int msg_size = 8192;
    System.out.println("Start init! ");
    kv.init(client_uri, wr_server_uri, rd_server_uri, phy_port, msg_size);
    System.out.println("successfully init! ");
  }
}
