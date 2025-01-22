package jni;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class OzoneDBJNITest {

    @Test
    void simpleTest() {
        OzoneDBJNI db = new OzoneDBJNI();
        String ozonedbHome = System.getenv("OZONEDB_HOME");
        db.openDB(ozonedbHome + "/src/config/local/shared_config_rocksdb_base.json");

        db.put("key1", "value1".getBytes());
        try {
            Thread.sleep(1000); // Sleep for 1 second
        } catch (InterruptedException e) {
        }
        String value = new String(db.get("key1"));
        assertEquals(value, "value1");

        db.remove("key1");
        db.closeDB();
    }
}
