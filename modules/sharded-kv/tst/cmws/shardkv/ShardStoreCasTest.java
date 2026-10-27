package cmws.shardkv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import cmws.kvstore.KVStore.CompareAndSet;
import cmws.kvstore.KVStore.ConditionalResult;
import cmws.kvstore.KVStore.Get;
import cmws.kvstore.KVStore.GetResult;
import cmws.kvstore.KVStore.KeyNotFound;
import cmws.kvstore.KVStore.PutIfAbsent;
import dslabs.framework.Client;
import dslabs.framework.Result;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.RunTests;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import dslabs.framework.testing.junit.UnreliableTests;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * End-to-end tests for conditional writes: clients, shard routing, Paxos groups and shard moves.
 * The workload is the one that matters for references: many writers race to advance one value.
 */
@Lab("5")
@Part(5)
public class ShardStoreCasTest extends ShardStoreBaseTest {
  private static final int GROUPS = 2, SERVERS = 3, MASTERS = 3, SHARDS = 10;

  /** Advance {@code key} by one using a read-then-CAS loop; returns the number of failed CASes. */
  private static int increment(Client client, String key) throws InterruptedException {
    int failures = 0;
    while (true) {
      client.sendCommand(new Get(key));
      Result current = client.getResult();
      String expected;
      int value;
      if (current instanceof KeyNotFound) {
        expected = null;
        value = 0;
      } else {
        expected = ((GetResult) current).value();
        value = Integer.parseInt(expected);
      }

      client.sendCommand(new CompareAndSet(key, expected, String.valueOf(value + 1)));
      ConditionalResult result = (ConditionalResult) client.getResult();
      if (result.success()) {
        return failures;
      }
      failures++;
    }
  }

  private void counterRace(int clients, long runMillis, int minMoves) throws Exception {
    setupStates(GROUPS, SERVERS, MASTERS, SHARDS);
    runState.start(runSettings);
    joinGroup(1, SERVERS);
    joinGroup(2, SERVERS);

    // Two counters that live in different shards, and shards that keep moving between the groups
    // while the clients race.
    String[] keys = {keyForShard(1), keyForShard(6)};
    AtomicInteger moves = new AtomicInteger();
    Thread mover =
        new Thread(
            () -> {
              java.util.Random rand = new java.util.Random();
              try {
                while (!Thread.interrupted()) {
                  Thread.sleep(250);
                  configController.sendCommand(
                      new cmws.shardmaster.ShardMaster.Move(
                          rand.nextInt(GROUPS) + 1, rand.nextInt(SHARDS) + 1));
                  configController.getResult();
                  moves.incrementAndGet();
                }
              } catch (InterruptedException ignored) {
              }
            });
    mover.start();

    List<Thread> threads = new ArrayList<>();
    AtomicInteger wins = new AtomicInteger();
    AtomicReference<Throwable> error = new AtomicReference<>();
    for (int c = 1; c <= clients; c++) {
      Client client = runState.addClient(client(c));
      Thread t =
          new Thread(
              () -> {
                try {
                  long deadline = System.currentTimeMillis() + runMillis;
                  while (System.currentTimeMillis() < deadline) {
                    for (String key : keys) {
                      increment(client, key);
                      wins.incrementAndGet();
                    }
                  }
                } catch (Throwable e) {
                  error.compareAndSet(null, e);
                }
              });
      threads.add(t);
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    mover.interrupt();
    mover.join();
    if (error.get() != null) {
      throw new AssertionError("client failed", error.get());
    }

    // Every successful CAS advanced a counter by exactly one and no update was lost or repeated.
    Client reader = runState.addClient(client(clients + 1));
    int total = 0;
    for (String key : keys) {
      reader.sendCommand(new Get(key));
      total += Integer.parseInt(((GetResult) reader.getResult()).value());
    }
    assertEquals(wins.get(), total);
    assertTrue("shards moved while the clients ran: " + moves.get(), moves.get() >= minMoves);
    runState.stop();
  }

  @Test(timeout = 90 * 1000)
  @TestDescription("Concurrent compare-and-set counters lose no updates, across shard moves")
  @Category(RunTests.class)
  @TestPointValue(15)
  public void test01ConcurrentCounters() throws Exception {
    counterRace(4, 4000, 5);
  }

  @Test(timeout = 120 * 1000)
  @TestDescription("Concurrent compare-and-set counters on an unreliable network")
  @Category({RunTests.class, UnreliableTests.class})
  @TestPointValue(15)
  public void test02ConcurrentCountersUnreliable() throws Exception {
    runSettings.networkDeliverRate(0.8);
    counterRace(3, 4000, 3);
  }

  @Test(timeout = 30 * 1000)
  @TestDescription("PutIfAbsent lets exactly one of several clients claim a name")
  @Category(RunTests.class)
  @TestPointValue(10)
  public void test03ExactlyOneClaimWins() throws Exception {
    setupStates(GROUPS, SERVERS, MASTERS, SHARDS);
    runState.start(runSettings);
    joinGroup(1, SERVERS);
    joinGroup(2, SERVERS);

    int claimants = 6;
    AtomicInteger winners = new AtomicInteger();
    AtomicReference<Throwable> error = new AtomicReference<>();
    List<Thread> threads = new ArrayList<>();
    for (int c = 1; c <= claimants; c++) {
      Client client = runState.addClient(client(c));
      final String mine = "owner-" + c;
      Thread t =
          new Thread(
              () -> {
                try {
                  client.sendCommand(new PutIfAbsent("name", mine));
                  if (((ConditionalResult) client.getResult()).success()) {
                    winners.incrementAndGet();
                  }
                } catch (Throwable e) {
                  error.compareAndSet(null, e);
                }
              });
      threads.add(t);
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    assertTrue(error.get() == null);
    assertEquals(1, winners.get());
    runState.stop();
  }
}
