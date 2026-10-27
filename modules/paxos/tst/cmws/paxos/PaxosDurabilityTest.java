package cmws.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cmws.atmostonce.AMOCommand;
import cmws.core.Effect;
import cmws.core.FileDurableStore;
import cmws.core.Persist;
import cmws.core.Send;
import cmws.kvstore.KVStore;
import dslabs.framework.Address;
import dslabs.framework.Message;
import dslabs.framework.testing.LocalAddress;
import dslabs.framework.testing.junit.DSLabsJUnitTest;
import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Crash recovery of the Paxos core: what is written to "disk" is all that survives. */
@Lab("5")
@Part(7)
@RunWith(DSLabsTestRunner.class)
public class PaxosDurabilityTest extends DSLabsJUnitTest {
  private static final Address CLIENT = new LocalAddress("client");

  /** Durable cores wired together, with a "disk" per node that outlives {@link #crashAll}. */
  private static final class DurableCluster {
    final Address[] servers = {
      new LocalAddress("s1"), new LocalAddress("s2"), new LocalAddress("s3")
    };
    final Map<Address, PaxosCore> cores = new LinkedHashMap<>();
    final Map<Address, List<Serializable>> disk = new HashMap<>();
    final Map<Address, List<AMOCommand>> executed = new HashMap<>();
    final Set<Address> dead = new HashSet<>();
    final Deque<Object[]> queue = new ArrayDeque<>();
    boolean dropAcceptReplies = false;

    DurableCluster() {
      for (Address a : servers) {
        cores.put(a, new PaxosCore(a, servers, true));
        disk.put(a, new ArrayList<>());
        executed.put(a, new ArrayList<>());
      }
    }

    void apply(Address at, List<Effect> effects) {
      // Persist first, exactly as the real host does: nothing is sent before it is on disk.
      for (Effect e : effects) {
        if (e instanceof Persist p) {
          disk.get(at).add(p.record());
        }
      }
      for (Effect e : effects) {
        if (e instanceof Send s) {
          queue.add(new Object[] {at, s.to(), s.message()});
        } else if (e instanceof Execute x) {
          executed.get(at).add(x.command());
        }
      }
    }

    void drain() {
      while (!queue.isEmpty()) {
        Object[] item = queue.poll();
        Address from = (Address) item[0], to = (Address) item[1];
        Message m = (Message) item[2];
        if (dead.contains(to) || dead.contains(from)) {
          continue;
        }
        if (dropAcceptReplies && m instanceof P2B) {
          continue;
        }
        PaxosCore c = cores.get(to);
        List<Effect> out;
        if (m instanceof P1A x) {
          out = c.onP1A(x, from);
        } else if (m instanceof P1B x) {
          out = c.onP1B(x, from);
        } else if (m instanceof P2A x) {
          out = c.onP2A(x, from);
        } else if (m instanceof P2B x) {
          out = c.onP2B(x, from);
        } else if (m instanceof HeartbeatRequest x) {
          out = c.onHeartbeatRequest(x, from);
        } else if (m instanceof HeartbeatReply x) {
          out = c.onHeartbeatReply(x, from);
        } else {
          throw new IllegalStateException("unexpected " + m);
        }
        apply(to, out);
      }
    }

    void startAll() {
      for (Address a : servers) {
        apply(a, cores.get(a).start());
      }
      drain();
    }

    Address leader() {
      for (Address a : servers) {
        if (!dead.contains(a) && cores.get(a).isActive()) {
          return a;
        }
      }
      return null;
    }

    void propose(Address leader, AMOCommand cmd) {
      apply(leader, cores.get(leader).propose(cmd));
      drain();
    }

    void heartbeats(Address leader, int rounds) {
      for (int i = 0; i < rounds; i++) {
        apply(leader, cores.get(leader).onHeartbeatTimer(new HeartbeatTimer()));
        drain();
      }
    }

    /** Every process dies; only the disks remain. Then each one restarts from its disk. */
    void crashAndRestartAll() {
      queue.clear();
      for (Address a : servers) {
        PaxosCore fresh = new PaxosCore(a, servers, true);
        fresh.restore(new ArrayList<>(disk.get(a)));
        cores.put(a, fresh);
        executed.put(a, new ArrayList<>()); // the application state is gone too
        apply(a, fresh.replay());
      }
    }
  }

  private static AMOCommand put(int i) {
    return new AMOCommand(new KVStore.Put("key" + i, "value" + i), i, CLIENT);
  }

  @Test(timeout = 30 * 1000)
  @TestPointValue(10)
  @TestDescription("After every replica restarts from its log, the same commands run in the same order")
  public void test01ReplayRebuildsTheApplication() {
    DurableCluster c = new DurableCluster();
    c.startAll();
    Address leader = c.leader();
    assertNotNull(leader);

    for (int i = 1; i <= 50; i++) {
      c.propose(leader, put(i));
    }
    c.heartbeats(leader, 3);
    List<AMOCommand> before = new ArrayList<>(c.executed.get(leader));
    assertEquals(50, before.size());
    for (Address a : c.servers) {
      assertEquals(before, c.executed.get(a));
    }

    c.crashAndRestartAll();
    for (Address a : c.servers) {
      assertEquals("replay at " + a, before, c.executed.get(a));
    }

    // The restarted group elects a leader and carries on from where it was.
    c.startAll();
    Address next = c.leader();
    assertNotNull(next);
    for (int i = 51; i <= 60; i++) {
      c.propose(next, put(i));
    }
    c.heartbeats(next, 3);
    assertEquals(60, c.executed.get(next).size());
    for (Address a : c.servers) {
      assertEquals(c.executed.get(next), c.executed.get(a));
    }
  }

  @Test(timeout = 30 * 1000)
  @TestPointValue(10)
  @TestDescription("A restarted replica still honours the promise it made before the crash")
  public void test02PromiseSurvivesRestart() {
    Address s1 = new LocalAddress("s1"), s2 = new LocalAddress("s2"), s3 = new LocalAddress("s3");
    Address[] servers = {s1, s2, s3};
    List<Serializable> disk = new ArrayList<>();

    PaxosCore core = new PaxosCore(s1, servers, true);
    // s1 promises the ballot of s3's election...
    List<Effect> out = core.onP1A(new P1A(new Ballot(7, s3)), s3);
    out.stream().filter(e -> e instanceof Persist).forEach(e -> disk.add(((Persist) e).record()));
    assertTrue(out.stream().anyMatch(e -> e instanceof Send s && s.message() instanceof P1B));

    // ...crashes, and comes back from its log.
    PaxosCore restarted = new PaxosCore(s1, servers, true);
    restarted.restore(disk);

    // An older election must now be refused: no promise, no vote.
    assertTrue(restarted.onP1A(new P1A(new Ballot(3, s2)), s2).isEmpty());
    assertTrue(
        restarted.onP2A(new P2A(new Ballot(3, s2), 1, put(1)), s2).isEmpty());
  }

  @Test(timeout = 30 * 1000)
  @TestPointValue(10)
  @TestDescription("A value accepted by a majority before a total crash is not lost")
  public void test03AcceptedValueIsRecovered() {
    DurableCluster c = new DurableCluster();
    c.startAll();
    Address leader = c.leader();
    assertNotNull(leader);

    // The value is accepted and written to disk by a majority, but the crash happens before anyone
    // learns that it was chosen.
    c.dropAcceptReplies = true;
    AMOCommand lost = put(1);
    c.propose(leader, lost);
    assertTrue(c.executed.get(leader).isEmpty());

    c.dropAcceptReplies = false;
    c.crashAndRestartAll();
    c.startAll();
    Address next = c.leader();
    assertNotNull(next);
    c.heartbeats(next, 3);

    // The new leader found the accepted value in a majority's log and decided it.
    assertEquals(List.of(lost), c.executed.get(next));
  }

  @Test(timeout = 30 * 1000)
  @TestPointValue(10)
  @TestDescription("The file log round-trips records and ignores a record cut short by a crash")
  public void test04FileStoreSurvivesTornWrite() throws IOException {
    Path dir = Files.createTempDirectory("wal-test");
    Path file = dir.resolve("wal");
    FileDurableStore store = new FileDurableStore(file);

    List<Serializable> first = List.of(new PaxosRecords.Promise(new Ballot(1, CLIENT)), "second");
    store.append(first);
    store.append(List.of("third"));
    store.close();
    assertEquals(List.of(first.get(0), "second", "third"), new FileDurableStore(file).readAll());

    // Simulate a crash in the middle of writing the next record: only part of it reached the disk.
    store = new FileDurableStore(file);
    store.append(List.of("fourth, never completely written"));
    store.close();
    try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
      raf.setLength(raf.length() - 5);
    }
    assertEquals(List.of(first.get(0), "second", "third"), new FileDurableStore(file).readAll());
  }
}
