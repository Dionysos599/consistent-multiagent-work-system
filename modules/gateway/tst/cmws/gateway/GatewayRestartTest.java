package cmws.gateway;

import static org.junit.Assert.assertEquals;

import cmws.kvstore.KVStore.CompareAndSet;
import cmws.kvstore.KVStore.ConditionalResult;
import cmws.kvstore.KVStore.Get;
import cmws.kvstore.KVStore.GetResult;
import cmws.kvstore.KVStore.KeyNotFound;
import cmws.kvstore.KVStore.Put;
import cmws.kvstore.KVStore.PutIfAbsent;
import dslabs.framework.testing.junit.DSLabsJUnitTest;
import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Test;
import org.junit.runner.RunWith;

/** A cluster with a data directory survives being shut down and started again. */
@Lab("5")
@Part(8)
@RunWith(DSLabsTestRunner.class)
public class GatewayRestartTest extends DSLabsJUnitTest {
  private static final long TIMEOUT = 30_000;

  private static StoreCluster start(Path dir) throws InterruptedException {
    return new StoreCluster(2, 3, 10, 4, dir);
  }

  @Test(timeout = 120 * 1000)
  @TestPointValue(20)
  @TestDescription("Data and references survive two full restarts of the cluster")
  public void test01DataSurvivesRestarts() throws Exception {
    Path dir = Files.createTempDirectory("cluster-data");

    try (StoreCluster first = start(dir)) {
      for (int i = 0; i < 20; i++) {
        first.execute(new Put("key-" + i, "v1-" + i), TIMEOUT);
      }
      first.execute(new PutIfAbsent("owner", "alice"), TIMEOUT);
      first.execute(new CompareAndSet("head", null, "c1"), TIMEOUT);
      first.execute(new CompareAndSet("head", "c1", "c2"), TIMEOUT);
    }

    try (StoreCluster second = start(dir)) {
      for (int i = 0; i < 20; i++) {
        assertEquals(new GetResult("v1-" + i), second.execute(new Get("key-" + i), TIMEOUT));
      }
      // The claim and the reference remember their state: the old version no longer matches.
      assertEquals(
          new ConditionalResult(false, "alice"),
          second.execute(new PutIfAbsent("owner", "mallory"), TIMEOUT));
      assertEquals(
          new ConditionalResult(false, "c2"),
          second.execute(new CompareAndSet("head", "c1", "bad"), TIMEOUT));
      assertEquals(
          new ConditionalResult(true, "c3"),
          second.execute(new CompareAndSet("head", "c2", "c3"), TIMEOUT));

      for (int i = 0; i < 20; i++) {
        second.execute(new Put("key-" + i, "v2-" + i), TIMEOUT);
      }
      assertEquals(new KeyNotFound(), second.execute(new Get("never-written"), TIMEOUT));
    }

    try (StoreCluster third = start(dir)) {
      for (int i = 0; i < 20; i++) {
        assertEquals(new GetResult("v2-" + i), third.execute(new Get("key-" + i), TIMEOUT));
      }
      assertEquals(new GetResult("c3"), third.execute(new Get("head"), TIMEOUT));
    }
  }
}
