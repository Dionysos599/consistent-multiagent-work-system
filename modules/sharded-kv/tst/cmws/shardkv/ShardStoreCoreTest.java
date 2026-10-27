package cmws.shardkv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import cmws.atmostonce.AMOCommand;
import cmws.core.Effect;
import cmws.core.Send;
import cmws.kvstore.KVStore;
import cmws.shardmaster.ShardMaster.ShardConfig;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.testing.LocalAddress;
import dslabs.framework.testing.junit.DSLabsJUnitTest;
import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Tests that drive {@link ShardStoreCore} directly, without a network or a Paxos group. */
@Lab("5")
@Part(3)
@RunWith(DSLabsTestRunner.class)
public class ShardStoreCoreTest extends DSLabsJUnitTest {
  private static final int NUM_SHARDS = 10;
  private static final Address CLIENT = new LocalAddress("client");
  private static final Address[] GROUP1 = {
    new LocalAddress("g1-1"), new LocalAddress("g1-2"), new LocalAddress("g1-3")
  };
  private static final Address[] GROUP2 = {
    new LocalAddress("g2-1"), new LocalAddress("g2-2"), new LocalAddress("g2-3")
  };
  private static final Address[] MASTERS = {
    new LocalAddress("sm1"), new LocalAddress("sm2"), new LocalAddress("sm3")
  };

  private static ShardStoreCore newCore() {
    return new ShardStoreCore(GROUP1[0], MASTERS, NUM_SHARDS, GROUP1, 1);
  }

  private static Set<Integer> shards(int from, int to) {
    Set<Integer> s = new HashSet<>();
    for (int i = from; i <= to; i++) {
      s.add(i);
    }
    return s;
  }

  /** Group 1 owns shards 1..10 and group 2 owns none. */
  private static ShardConfig config(int num) {
    Map<Integer, Pair<Set<Address>, Set<Integer>>> info = new HashMap<>();
    info.put(1, new ImmutablePair<>(Set.of(GROUP1), shards(1, NUM_SHARDS)));
    info.put(2, new ImmutablePair<>(Set.of(GROUP2), new HashSet<>()));
    return new ShardConfig(num, info);
  }

  private static AMOCommand internal(Command c) {
    return new AMOCommand(c, 0, GROUP1[0]);
  }

  private static boolean proposes(List<Effect> effects) {
    return effects.stream().anyMatch(e -> e instanceof Propose);
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("An acknowledgement before any configuration is ignored")
  public void test01AckWithoutConfigurationIsIgnored() {
    ShardStoreCore core = newCore();
    List<Effect> effects =
        core.onDecision(internal(new ProcessShardMoveAck(new ShardMoveAck(1, 2))));
    assertTrue(effects.isEmpty());
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A shard move for a configuration not installed yet is not proposed")
  public void test02EarlyShardMoveIsDropped() {
    ShardStoreCore core = newCore();
    core.onDecision(internal(new StartReconfiguration(config(0))));

    ShardMove early = new ShardMove(2, 1, GROUP2, new HashMap<>());
    assertFalse(proposes(core.onShardMove(early)));

    // A move for the configuration we are in is proposed as usual.
    ShardMove current = new ShardMove(2, 0, GROUP2, new HashMap<>());
    assertTrue(proposes(core.onShardMove(current)));
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A stale reconfiguration decided late does not roll the group back")
  public void test03StaleReconfigurationIsIgnored() {
    ShardStoreCore core = newCore();
    core.onDecision(internal(new StartReconfiguration(config(0))));
    core.onDecision(internal(new StartReconfiguration(config(1))));
    core.onDecision(internal(new StartReconfiguration(config(0)))); // duplicate, decided late

    // Still in configuration 1: a request tagged with 1 is accepted for the log, one tagged with 0
    // is rejected.
    AMOCommand put = new AMOCommand(new KVStore.Put("key-1", "v"), 0, CLIENT);
    List<Effect> current = core.onClientRequest(new ShardStoreRequest(put, 1), CLIENT);
    assertTrue(proposes(current));

    List<Effect> old = core.onClientRequest(new ShardStoreRequest(put, 0), CLIENT);
    assertFalse(proposes(old));
    assertTrue(old.get(0) instanceof Send s && ((ShardStoreReply) s.message()).hasError());
  }

  @Test(timeout = 5 * 1000)
  @TestPointValue(5)
  @TestDescription("A decided client command is re-checked against current ownership")
  public void test04DecidedCommandForAShardWeNoLongerOwnIsRejected() {
    ShardStoreCore core = newCore();
    // Group 1 never received any configuration, so it owns nothing even though the command was
    // decided in its log.
    AMOCommand put = new AMOCommand(new KVStore.Put("key-1", "v"), 0, CLIENT);
    List<Effect> effects = core.onDecision(put);
    assertEquals(1, effects.size());
    Send reply = (Send) effects.get(0);
    assertEquals(CLIENT, reply.to());
    assertTrue(((ShardStoreReply) reply.message()).hasError());
  }
}
