package cmws.atmostonce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import cmws.kvstore.KVStore;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.framework.testing.LocalAddress;
import dslabs.framework.testing.junit.DSLabsJUnitTest;
import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import lombok.Data;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Unit tests for the at-most-once wrapper. */
@Lab("5")
@Part(1)
@RunWith(DSLabsTestRunner.class)
public class AMOApplicationTest extends DSLabsJUnitTest {
  private static final Address CLIENT = new LocalAddress("client1");
  private static final Address OTHER = new LocalAddress("client2");

  /** An application that counts how often it is executed. */
  @Data
  static final class Counter implements Application {
    private int executions;

    @Override
    public Result execute(Command command) {
      executions++;
      return new Count(executions);
    }
  }

  @Data
  static final class Increment implements Command {}

  @Data
  static final class Count implements Result {
    private final int value;
  }

  private static AMOCommand inc(Address client, int seq) {
    return new AMOCommand(new Increment(), seq, client);
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A duplicate of an executed command is answered from the cache")
  public void test01DuplicateReturnsCachedResult() {
    Counter counter = new Counter();
    AMOApplication<Counter> app = new AMOApplication<>(counter);

    AMOResult first = app.execute(inc(CLIENT, 1));
    AMOResult second = app.execute(inc(CLIENT, 1));

    assertEquals(first, second);
    assertEquals(1, counter.executions());
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A late command from a client that moved on is never executed")
  public void test02StaleCommandIsNotExecuted() {
    Counter counter = new Counter();
    AMOApplication<Counter> app = new AMOApplication<>(counter);

    app.execute(inc(CLIENT, 1));
    app.execute(inc(CLIENT, 3));
    assertEquals(2, counter.executions());

    // Sequence number 2 was never seen and is older than 3: the client has given up on it.
    AMOResult late = app.execute(inc(CLIENT, 2));
    assertEquals(new StaleCommand(), late.result());
    assertEquals(2, counter.executions());

    // Retrying the late command again still does not run it.
    app.execute(inc(CLIENT, 2));
    assertEquals(2, counter.executions());
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("Clients are tracked independently")
  public void test03ClientsAreIndependent() {
    Counter counter = new Counter();
    AMOApplication<Counter> app = new AMOApplication<>(counter);

    app.execute(inc(CLIENT, 5));
    // A low sequence number from a different client is not stale.
    AMOResult other = app.execute(inc(OTHER, 1));
    assertNotEquals(new StaleCommand(), other.result());
    assertEquals(2, counter.executions());
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A late put does not overwrite newer state")
  public void test04StalePutDoesNotOverwrite() {
    KVStore kv = new KVStore();
    AMOApplication<KVStore> app = new AMOApplication<>(kv);

    app.execute(new AMOCommand(new KVStore.Put("k", "new"), 5, CLIENT));
    AMOResult late = app.execute(new AMOCommand(new KVStore.Put("k", "old"), 3, CLIENT));

    assertEquals(new KVStore.PutOk(), late.result());
    assertEquals(new KVStore.GetResult("new"), kv.execute(new KVStore.Get("k")));
  }
}
