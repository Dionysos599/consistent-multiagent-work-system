package cmws.paxos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import cmws.atmostonce.AMOCommand;
import cmws.core.Effect;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests that drive {@link PaxosCore} directly. Because the core is a pure state machine, a handful
 * of cores plus a few lines of message routing make a deterministic cluster: no threads, no timers,
 * no framework.
 */
@Lab("5")
@Part(2)
@RunWith(DSLabsTestRunner.class)
public class PaxosCoreTest extends DSLabsJUnitTest {
  private static final Address CLIENT = new LocalAddress("client");

  /** A cluster of cores wired together by an in-memory queue. */
  private static final class Cluster {
    final Address[] servers;
    final Map<Address, PaxosCore> cores = new LinkedHashMap<>();
    final Map<Address, List<AMOCommand>> executed = new LinkedHashMap<>();
    final Set<Address> dead = new HashSet<>();
    final Deque<Object[]> queue = new ArrayDeque<>();
    int largestHeartbeat = 0;

    Cluster(int size) {
      servers = new Address[size];
      for (int i = 0; i < size; i++) {
        servers[i] = new LocalAddress("s" + (i + 1));
      }
      for (Address a : servers) {
        cores.put(a, new PaxosCore(a, servers));
        executed.put(a, new ArrayList<>());
      }
    }

    void apply(Address at, List<Effect> effects) {
      for (Effect e : effects) {
        if (e instanceof Send s) {
          if (s.message() instanceof HeartbeatRequest hb && !dead.contains(s.to())) {
            largestHeartbeat = Math.max(largestHeartbeat, hb.log().size());
          }
          queue.add(new Object[] {at, s.to(), s.message()});
        } else if (e instanceof Execute x) {
          executed.get(at).add(x.command());
        }
      }
    }

    void drain() {
      while (!queue.isEmpty()) {
        Object[] item = queue.poll();
        Address from = (Address) item[0];
        Address to = (Address) item[1];
        Message m = (Message) item[2];
        if (dead.contains(to) || dead.contains(from)) {
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
        if (!dead.contains(a)) {
          apply(a, cores.get(a).start());
        }
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

    void heartbeat(Address leader) {
      apply(leader, cores.get(leader).onHeartbeatTimer(new HeartbeatTimer()));
      drain();
    }

    void propose(Address leader, AMOCommand cmd) {
      apply(leader, cores.get(leader).propose(cmd));
      drain();
    }
  }

  private static AMOCommand put(int i) {
    return new AMOCommand(new KVStore.Put("key" + i, "value" + i), i, CLIENT);
  }

  @Test(timeout = 30 * 1000)
  @TestPointValue(10)
  @TestDescription("A follower catches up through bounded heartbeats while a replica is down")
  public void test01HeartbeatsStayBoundedAndFollowerCatchesUp() {
    Cluster cluster = new Cluster(3);
    cluster.dead.add(cluster.servers[2]); // never reachable, so nothing is ever garbage-collected
    cluster.startAll();

    Address leader = cluster.leader();
    assertNotNull("a leader is elected with two of three replicas", leader);
    Address follower = cluster.servers[0].equals(leader) ? cluster.servers[1] : cluster.servers[0];

    int commands = 1000;
    for (int i = 1; i <= commands; i++) {
      cluster.propose(leader, put(i));
    }
    assertEquals(commands, cluster.executed.get(leader).size());
    assertTrue("followers learn decisions only through heartbeats", cluster.executed.get(follower).size() < commands);

    // 1000 entries at no more than 128 per heartbeat: well under 20 rounds are needed.
    for (int round = 0; round < 20; round++) {
      cluster.heartbeat(leader);
    }

    assertEquals(cluster.executed.get(leader), cluster.executed.get(follower));
    assertTrue(
        "heartbeat carried " + cluster.largestHeartbeat + " entries",
        cluster.largestHeartbeat <= 128);

    // Once the follower is caught up its heartbeats carry nothing new.
    cluster.largestHeartbeat = 0;
    cluster.heartbeat(leader);
    assertEquals(0, cluster.largestHeartbeat);
  }

  @Test(timeout = 30 * 1000)
  @TestPointValue(10)
  @TestDescription("A retry is suppressed only while its first copy is still unexecuted")
  public void test02RetryOfExecutedCommandIsProposedAgain() {
    Cluster cluster = new Cluster(3);
    cluster.dead.add(cluster.servers[2]);
    cluster.startAll();
    Address leader = cluster.leader();
    PaxosCore core = cluster.cores.get(leader);

    AMOCommand cmd = put(1);

    // Proposed but not decided yet (the accept messages are still in flight): a second copy of the
    // same command adds nothing to the log.
    cluster.apply(leader, core.propose(cmd));
    int afterFirst = core.lastNonEmpty();
    cluster.apply(leader, core.propose(cmd));
    assertEquals(afterFirst, core.lastNonEmpty());

    cluster.drain();
    assertEquals(1, cluster.executed.get(leader).size());

    // The command has now been executed. A caller that resends it (the shard store resends moves
    // that were decided too early to take effect) must get a fresh slot rather than being silently
    // dropped because an old copy still sits in the log.
    int before = core.lastNonEmpty();
    cluster.apply(leader, core.propose(cmd));
    cluster.drain();
    assertEquals(before + 1, core.lastNonEmpty());
    assertEquals(2, cluster.executed.get(leader).size());
  }
}
