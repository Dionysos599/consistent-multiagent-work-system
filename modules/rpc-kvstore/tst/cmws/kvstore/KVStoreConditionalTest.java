package cmws.kvstore;

import static org.junit.Assert.assertEquals;

import cmws.atmostonce.AMOApplication;
import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.StaleCommand;
import cmws.kvstore.KVStore.CompareAndSet;
import cmws.kvstore.KVStore.ConditionalResult;
import cmws.kvstore.KVStore.Get;
import cmws.kvstore.KVStore.GetResult;
import cmws.kvstore.KVStore.KeyNotFound;
import cmws.kvstore.KVStore.Put;
import cmws.kvstore.KVStore.PutIfAbsent;
import dslabs.framework.testing.LocalAddress;
import dslabs.framework.testing.junit.DSLabsJUnitTest;
import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Semantics of the conditional writes {@code PutIfAbsent} and {@code CompareAndSet}. */
@Lab("5")
@Part(4)
@RunWith(DSLabsTestRunner.class)
public class KVStoreConditionalTest extends DSLabsJUnitTest {
  private KVStore kv;

  @Before
  public void setup() {
    kv = new KVStore();
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("PutIfAbsent stores once and reports the value in the way afterwards")
  public void test01PutIfAbsent() {
    assertEquals(new ConditionalResult(true, "a"), kv.execute(new PutIfAbsent("k", "a")));
    assertEquals(new ConditionalResult(false, "a"), kv.execute(new PutIfAbsent("k", "b")));
    assertEquals(new GetResult("a"), kv.execute(new Get("k")));
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("CompareAndSet replaces the value only when it matches")
  public void test02CompareAndSet() {
    kv.execute(new Put("k", "v1"));

    assertEquals(new ConditionalResult(false, "v1"), kv.execute(new CompareAndSet("k", "other", "x")));
    assertEquals(new GetResult("v1"), kv.execute(new Get("k")));

    assertEquals(new ConditionalResult(true, "v2"), kv.execute(new CompareAndSet("k", "v1", "v2")));
    assertEquals(new GetResult("v2"), kv.execute(new Get("k")));

    // The old value no longer matches: this is what stops two writers from both winning.
    assertEquals(new ConditionalResult(false, "v2"), kv.execute(new CompareAndSet("k", "v1", "v3")));
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("CompareAndSet on a missing key: null expects absence")
  public void test03CompareAndSetOnMissingKey() {
    assertEquals(new ConditionalResult(false, null), kv.execute(new CompareAndSet("k", "v", "x")));
    assertEquals(new KeyNotFound(), kv.execute(new Get("k")));

    assertEquals(new ConditionalResult(true, "x"), kv.execute(new CompareAndSet("k", null, "x")));
    assertEquals(new ConditionalResult(false, "x"), kv.execute(new CompareAndSet("k", null, "y")));
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A late CompareAndSet from a client that moved on is not executed")
  public void test04LateCompareAndSetIsNotExecuted() {
    AMOApplication<KVStore> app = new AMOApplication<>(kv);
    LocalAddress client = new LocalAddress("client");

    app.execute(new AMOCommand(new Put("k", "v1"), 1, client));
    app.execute(new AMOCommand(new Get("k"), 5, client));

    // Sequence 3 never arrived in time and the client has already issued sequence 5.
    AMOCommand late = new AMOCommand(new CompareAndSet("k", "v1", "late"), 3, client);
    assertEquals(new StaleCommand(), app.execute(late).result());
    assertEquals(new GetResult("v1"), kv.execute(new Get("k")));
  }
}
