package cmws.shardkv;

import cmws.atmostonce.AMOApplication;
import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import cmws.core.Effect;
import cmws.core.Send;
import cmws.core.SetTimer;
import cmws.kvstore.KVStore;
import cmws.kvstore.KVStore.SingleKeyCommand;
import cmws.paxos.PaxosRequest;
import cmws.shardmaster.ShardMaster.Query;
import cmws.shardmaster.ShardMaster.ShardConfig;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * One replica of a shard store group, as a pure state machine.
 *
 * <p>Client requests and every configuration change (start of a reconfiguration, an incoming shard
 * move, the acknowledgement of a move we sent) are first proposed to the group's Paxos log and only
 * take effect when they come back as decisions, so all replicas of the group apply the same
 * changes in the same order. The core returns {@link Effect}s (messages, timers, proposals) instead
 * of performing them; {@link ShardStoreServer} adapts it to the test framework.
 */
@ToString
@EqualsAndHashCode
final class ShardStoreCore implements Serializable {
  private static final int INITIAL_CONFIG = 0;

  private final Address self;
  private final Address[] shardMasters;
  private final int numShards;
  private final Address[] group;
  private final int groupId;

  private ShardConfig shardConfig;
  private final Map<Integer, AMOApplication<Application>> shardToApp;
  private boolean isReconfiguring;
  private Map<Integer, Set<Integer>> shardToSend; // groupId -> shards to send
  private Set<Integer> shardToReceive;

  /** True while decisions from before a restart are being replayed. */
  private boolean replaying;

  ShardStoreCore(
      Address self, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    this.self = self;
    this.shardMasters = shardMasters;
    this.numShards = numShards;
    this.group = group;
    this.groupId = groupId;
    this.shardToApp = new HashMap<>();
    this.isReconfiguring = false;
    this.shardToSend = new HashMap<>();
    this.shardToReceive = new HashSet<>();
  }

  Address[] group() {
    return group;
  }

  /**
   * Decisions that are replayed after a restart rebuild the state but must not talk to anyone: the
   * clients and groups they would answer belong to the previous run. Whatever the other side still
   * needs is resent by the normal retry timers.
   */
  void beginReplay() {
    replaying = true;
  }

  void endReplay() {
    replaying = false;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Events
   * ---------------------------------------------------------------------------------------------*/
  List<Effect> start() {
    List<Effect> out = new ArrayList<>();
    queryForNewConfig(out);
    out.add(new SetTimer(new QueryTimer(), QueryTimer.QUERY_RETRY_MILLIS));
    return out;
  }

  /** A client request: answer it directly, reject it, or propose it to the group's log. */
  List<Effect> onClientRequest(ShardStoreRequest m, Address sender) {
    List<Effect> out = new ArrayList<>();
    if (shardConfig == null || m.configNum() != shardConfig.configNum() || isReconfiguring) {
      out.add(new Send(errorReply(), sender));
      return out;
    }

    AMOCommand amoCmd = m.amoCommand();
    if (!(amoCmd.command() instanceof SingleKeyCommand)) {
      out.add(new Send(errorReply(), sender));
      return out;
    }

    SingleKeyCommand skc = (SingleKeyCommand) amoCmd.command();
    int shard = keyToShard(skc.key());

    if (!ownsShard(shard)) {
      out.add(new Send(errorReply(), sender));
      return out;
    }

    AMOApplication<Application> app = shardToApp.get(shard);
    if (app == null) {
      out.add(new Send(errorReply(), sender));
      return out;
    }

    if (app.alreadyExecuted(amoCmd)) {
      AMOResult result = app.execute(amoCmd);
      out.add(new Send(new ShardStoreReply(result, shardConfig.configNum(), false), sender));
      return out;
    }

    out.add(new Propose(amoCmd));
    return out;
  }

  /** A reply from the shard master; a configuration that is exactly the next one is proposed. */
  List<Effect> onConfigReply(Result result) {
    List<Effect> out = new ArrayList<>();
    if (isReconfiguring || !(result instanceof ShardConfig)) {
      return out;
    }
    ShardConfig newConfig = (ShardConfig) result;
    if ((shardConfig == null && newConfig.configNum() == INITIAL_CONFIG)
        || (shardConfig != null && newConfig.configNum() == shardConfig.configNum() + 1)) {
      out.add(new Propose(internalCommand(new StartReconfiguration(newConfig))));
    }
    return out;
  }

  List<Effect> onShardMove(ShardMove m) {
    List<Effect> out = new ArrayList<>();
    // A move for a configuration this group has not installed yet cannot be applied. Proposing it
    // would only put a no-op decision in the log ahead of our own StartReconfiguration; drop it and
    // rely on the sender's ShardMoveTimer to deliver it again.
    if (shardConfig == null || m.configNum() > shardConfig.configNum()) {
      return out;
    }
    out.add(new Propose(internalCommand(new ProcessShardMove(m))));
    return out;
  }

  List<Effect> onShardMoveAck(ShardMoveAck m) {
    List<Effect> out = new ArrayList<>();
    out.add(new Propose(internalCommand(new ProcessShardMoveAck(m))));
    return out;
  }

  /** A command the group's Paxos log has decided: apply it. */
  List<Effect> onDecision(AMOCommand amoCmd) {
    List<Effect> out = new ArrayList<>();
    Command cmd = amoCmd.command();
    if (cmd instanceof SingleKeyCommand) {
      applyClientCommand(amoCmd, (SingleKeyCommand) cmd, out);
    } else if (cmd instanceof StartReconfiguration) {
      applyStartReconfiguration((StartReconfiguration) cmd, out);
    } else if (cmd instanceof ProcessShardMove) {
      applyShardMove(((ProcessShardMove) cmd).shardMove(), out);
    } else if (cmd instanceof ProcessShardMoveAck) {
      applyShardMoveAck(((ProcessShardMoveAck) cmd).ack());
    }
    if (replaying) {
      out.removeIf(e -> e instanceof Send);
    }
    return out;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timers
   * ---------------------------------------------------------------------------------------------*/
  List<Effect> onQueryTimer(QueryTimer t) {
    List<Effect> out = new ArrayList<>();
    if (!isReconfiguring) {
      queryForNewConfig(out);
    }
    out.add(new SetTimer(t, QueryTimer.QUERY_RETRY_MILLIS));
    return out;
  }

  List<Effect> onShardMoveTimer(ShardMoveTimer t) {
    List<Effect> out = new ArrayList<>();
    if (!isReconfiguring || shardConfig == null || t.configNum() != shardConfig.configNum()) {
      return out;
    }
    if (!shardToSend.isEmpty()) {
      sendShards(out);
      out.add(new SetTimer(t, ShardMoveTimer.MOVE_RETRY_MILLIS));
    }
    return out;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Applying decisions
   * ---------------------------------------------------------------------------------------------*/
  private void applyClientCommand(AMOCommand amoCmd, SingleKeyCommand skc, List<Effect> out) {
    int shard = keyToShard(skc.key());

    // The decision can be applied after a configuration change that took the shard away (the
    // request was proposed under an older configuration), so ownership must be re-checked here.
    // On any mismatch tell the client to refresh its configuration instead of executing.
    AMOApplication<Application> app = shardToApp.get(shard);
    boolean owns = !isReconfiguring && ownsShard(shard) && app != null;
    if (!owns) {
      out.add(new Send(errorReply(), amoCmd.sender()));
      return;
    }

    AMOResult result = app.execute(amoCmd);
    out.add(new Send(new ShardStoreReply(result, shardConfig.configNum(), false), amoCmd.sender()));
  }

  private void applyStartReconfiguration(StartReconfiguration cmd, List<Effect> out) {
    // Decisions are applied in log order on every replica, so this check is deterministic. A
    // duplicate or stale proposal (the same configuration proposed twice, or one decided after a
    // newer configuration was installed) must not restart or regress a reconfiguration.
    int expected = shardConfig == null ? INITIAL_CONFIG : shardConfig.configNum() + 1;
    if (isReconfiguring || cmd.newConfig().configNum() != expected) {
      return;
    }

    isReconfiguring = true;
    ShardConfig oldConfig = shardConfig;
    shardConfig = cmd.newConfig();

    Set<Integer> newShards = new HashSet<>();
    if (shardConfig.groupInfo().containsKey(groupId)) {
      newShards = shardConfig.groupInfo().get(groupId).getRight();
    }

    // Handle initial configuration
    if (oldConfig == null) {
      for (int shard : newShards) {
        shardToApp.put(shard, new AMOApplication<>(new KVStore()));
      }
      isReconfiguring = false;
      return;
    }

    Set<Integer> oldShards = new HashSet<>();
    if (oldConfig.groupInfo().containsKey(groupId)) {
      oldShards = oldConfig.groupInfo().get(groupId).getRight();
    }

    shardToSend.clear();
    shardToReceive.clear();

    Set<Integer> toSend = new HashSet<>(oldShards);
    toSend.removeAll(newShards);

    for (int shard : toSend) {
      for (var entry : shardConfig.groupInfo().entrySet()) {
        if (entry.getValue().getRight().contains(shard)) {
          shardToSend.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(shard);
          break;
        }
      }
    }

    shardToReceive = new HashSet<>(newShards);
    shardToReceive.removeAll(oldShards);

    if (shardToSend.isEmpty() && shardToReceive.isEmpty()) {
      isReconfiguring = false;
    } else {
      sendShards(out);
      out.add(
          new SetTimer(new ShardMoveTimer(shardConfig.configNum()), ShardMoveTimer.MOVE_RETRY_MILLIS));
    }
  }

  private void applyShardMove(ShardMove move, List<Effect> out) {
    if (shardConfig == null || move.configNum() > shardConfig.configNum()) {
      return;
    }

    if (isReconfiguring && move.configNum() == shardConfig.configNum()) {
      for (var entry : move.shardToApp().entrySet()) {
        int shard = entry.getKey();
        if (shardToReceive.contains(shard)) {
          shardToApp.put(shard, entry.getValue());
          shardToReceive.remove(shard);
        }
      }
    }

    // Acknowledge every move for a configuration we have already passed or just installed. If the
    // acknowledgement is lost the sender keeps retrying; once this group has moved on to a newer
    // configuration it must still answer, otherwise the sender would wait forever and block the
    // reconfiguration that depends on it.
    ShardMoveAck ack = new ShardMoveAck(move.configNum(), groupId);
    for (Address server : move.senderGroup()) {
      out.add(new Send(ack, server));
    }

    finishReconfigurationIfDone();
  }

  private void applyShardMoveAck(ShardMoveAck ack) {
    if (shardConfig == null
        || ack.configNum() != shardConfig.configNum()
        || !isReconfiguring
        || !shardToSend.containsKey(ack.groupId())) {
      return;
    }

    Set<Integer> ackedShards = shardToSend.get(ack.groupId());
    if (ackedShards != null) {
      for (int shard : ackedShards) {
        shardToApp.remove(shard);
      }
      shardToSend.remove(ack.groupId());
    }

    finishReconfigurationIfDone();
  }

  private void finishReconfigurationIfDone() {
    if (shardToSend.isEmpty() && shardToReceive.isEmpty()) {
      isReconfiguring = false;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  private boolean ownsShard(int shard) {
    return shardConfig != null
        && shardConfig.groupInfo().containsKey(groupId)
        && shardConfig.groupInfo().get(groupId).getRight().contains(shard);
  }

  private int keyToShard(String key) {
    return ShardStoreNode.keyToShard(key, numShards);
  }

  private ShardStoreReply errorReply() {
    return new ShardStoreReply(null, shardConfig != null ? shardConfig.configNum() : -1, true);
  }

  /** Wrap a group-internal command so that it can travel through the Paxos log. */
  private AMOCommand internalCommand(Command command) {
    return new AMOCommand(command, 0, self);
  }

  private void queryForNewConfig(List<Effect> out) {
    int nextConfig = (shardConfig != null) ? shardConfig.configNum() + 1 : INITIAL_CONFIG;
    AMOCommand queryCmd = new AMOCommand(new Query(nextConfig), -1, self);
    for (Address shardMaster : shardMasters) {
      out.add(new Send(new PaxosRequest(queryCmd), shardMaster));
    }
  }

  private void sendShards(List<Effect> out) {
    for (var entry : shardToSend.entrySet()) {
      int destGroupId = entry.getKey();
      Set<Integer> shards = entry.getValue();

      Map<Integer, AMOApplication> shardsToSend = new HashMap<>();
      for (int shard : shards) {
        AMOApplication app = shardToApp.get(shard);
        if (app != null) {
          shardsToSend.put(shard, app);
        } else {
          // This should not happen, but provide a fallback
          shardsToSend.put(shard, new AMOApplication<>(new KVStore()));
        }
      }

      Set<Address> destServers = null;
      for (var groupEntry : shardConfig.groupInfo().entrySet()) {
        if (groupEntry.getKey() == destGroupId) {
          destServers = groupEntry.getValue().getLeft();
          break;
        }
      }

      if (destServers != null && !shardsToSend.isEmpty()) {
        ShardMove move = new ShardMove(groupId, shardConfig.configNum(), group, shardsToSend);
        for (Address server : destServers) {
          out.add(new Send(move, server));
        }
      }
    }
  }
}
