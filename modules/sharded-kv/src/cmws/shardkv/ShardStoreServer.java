package cmws.shardkv;

import cmws.atmostonce.AMOApplication;
import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import cmws.kvstore.KVStore;
import cmws.kvstore.KVStore.SingleKeyCommand;
import cmws.paxos.PaxosDecision;
import cmws.paxos.PaxosReply;
import cmws.paxos.PaxosRequest;
import cmws.paxos.PaxosServer;
import cmws.shardmaster.ShardMaster.Query;
import cmws.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private static final String PAXOS_ADDRESS_ID = "paxos";

  private final Address[] group;
  private final int groupId;
  private Address paxosAddress;

  private ShardConfig shardConfig;
  private Map<Integer, AMOApplication<Application>> shardToApp;
  private boolean isReconfiguring;
  private Map<Integer, Set<Integer>> shardToSend; // groupId -> shards to send
  private Set<Integer> shardToReceive;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  ShardStoreServer(
      Address address, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    super(address, shardMasters, numShards);
    this.group = group;
    this.groupId = groupId;
    this.shardToApp = new HashMap<>();
    this.isReconfiguring = false;
    this.shardToSend = new HashMap<>();
    this.shardToReceive = new HashSet<>();
  }

  @Override
  public void init() {
    paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

    Address[] paxosAddresses = new Address[group.length];
    for (int i = 0; i < paxosAddresses.length; i++) {
      paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
    }

    PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
    addSubNode(paxosServer);
    paxosServer.init();

    // Initial configuration query
    queryForNewConfig();
    set(new QueryTimer(), QueryTimer.QUERY_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    if (shardConfig == null || m.configNum() != shardConfig.configNum() || isReconfiguring) {
      send(
          new ShardStoreReply(null, shardConfig != null ? shardConfig.configNum() : -1, true),
          sender);
      return;
    }

    AMOCommand amoCmd = m.amoCommand();
    if (!(amoCmd.command() instanceof SingleKeyCommand)) {
      send(new ShardStoreReply(null, shardConfig.configNum(), true), sender);
      return;
    }

    SingleKeyCommand skc = (SingleKeyCommand) amoCmd.command();
    int shard = keyToShard(skc.key());

    boolean ownShard = false;
    if (shardConfig.groupInfo().containsKey(groupId)) {
      ownShard = shardConfig.groupInfo().get(groupId).getRight().contains(shard);
    }

    if (!ownShard) {
      send(new ShardStoreReply(null, shardConfig.configNum(), true), sender);
      return;
    }

    AMOApplication<Application> app = shardToApp.get(shard);
    if (app == null) {
      send(new ShardStoreReply(null, shardConfig.configNum(), true), sender);
      return;
    }

    if (app.alreadyExecuted(amoCmd)) {
      AMOResult result = app.execute(amoCmd);
      send(new ShardStoreReply(result, shardConfig.configNum(), false), sender);
      return;
    }

    handleMessage(new PaxosRequest(amoCmd), paxosAddress);
  }

  // Handle reply from ShardMaster
  private void handlePaxosReply(PaxosReply m, Address sender) {
    if (isReconfiguring || !(m.result().result() instanceof ShardConfig)) {
      return;
    }
    Result result = m.result().result();
    ShardConfig newConfig = (ShardConfig) result;
   if ((shardConfig == null && newConfig.configNum() == 0)
        || (shardConfig != null && newConfig.configNum() == shardConfig.configNum() + 1)) {
      StartReconfiguration startReconfig = new StartReconfiguration(newConfig);
      AMOCommand cmd = new AMOCommand(startReconfig, 0, address());
      handleMessage(new PaxosRequest(cmd), paxosAddress);
    }
  }

  private void handleShardMove(ShardMove m, Address sender) {
    // Propose to Paxos
    ProcessShardMove processMove = new ProcessShardMove(m);
    AMOCommand cmd = new AMOCommand(processMove, 0, address());
    handleMessage(new PaxosRequest(cmd), paxosAddress);
  }

  private void handleShardMoveAck(ShardMoveAck m, Address sender) {
    ProcessShardMoveAck processAck = new ProcessShardMoveAck(m);
    AMOCommand cmd = new AMOCommand(processAck, 0, address());
    handleMessage(new PaxosRequest(cmd), paxosAddress);
  }

  // Handle decisions from Paxos
  private void handlePaxosDecision(PaxosDecision m, Address sender) {
    AMOCommand amoCmd = m.command();
    Command cmd = amoCmd.command();
    if (cmd instanceof SingleKeyCommand) {
      // Avoid executing client request
      if (isReconfiguring) {
        return;
      }
      SingleKeyCommand skc = (SingleKeyCommand) cmd;
      int shard = keyToShard(skc.key());

      AMOApplication<Application> app = shardToApp.get(shard);
      AMOResult result = app.execute(amoCmd);
      send(new ShardStoreReply(result, shardConfig.configNum(), false), amoCmd.sender());
    } else if (cmd instanceof StartReconfiguration) {
      handleStartReconfiguration((StartReconfiguration) cmd);
    } else if (cmd instanceof ProcessShardMove) {
      handleProcessShardMove(((ProcessShardMove) cmd).shardMove());
    } else if (cmd instanceof ProcessShardMoveAck) {
      handleProcessShardMoveAck(((ProcessShardMoveAck) cmd).ack());
    }
  }

  private void handleStartReconfiguration(StartReconfiguration cmd) {
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
    if (oldConfig != null && oldConfig.groupInfo().containsKey(groupId)) {
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
      sendShards();
      set(new ShardMoveTimer(shardConfig.configNum()), ShardMoveTimer.MOVE_RETRY_MILLIS);
    }
  }

  private void handleProcessShardMove(ShardMove move) {
    if (shardConfig == null || move.configNum() > shardConfig.configNum()) {
      return;
    }

    if (move.configNum() <= shardConfig.configNum() && !isReconfiguring) {
      ShardMoveAck ack = new ShardMoveAck(move.configNum(), groupId);
      broadcast(ack, move.senderGroup());
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
      ShardMoveAck ack = new ShardMoveAck(shardConfig.configNum(), groupId);
      broadcast(ack, move.senderGroup());
    }

    if (shardToSend.isEmpty() && shardToReceive.isEmpty()) {
      isReconfiguring = false;
    }
  }

  private void handleProcessShardMoveAck(ShardMoveAck ack) {
    if (ack.configNum() != shardConfig.configNum()
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

    if (shardToSend.isEmpty() && shardToReceive.isEmpty()) {
      isReconfiguring = false;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onQueryTimer(QueryTimer t) {
    if (!isReconfiguring) {
      queryForNewConfig();
    }
    set(t, QueryTimer.QUERY_RETRY_MILLIS);
  }

  private void onShardMoveTimer(ShardMoveTimer t) {
    if (!isReconfiguring || shardConfig == null || t.configNum() != shardConfig.configNum()) {
      return;
    }
    if (!shardToSend.isEmpty()) {
      sendShards();
      set(t, ShardMoveTimer.MOVE_RETRY_MILLIS);
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  private void queryForNewConfig() {
    int nextConfig = (shardConfig != null) ? shardConfig.configNum() + 1 : 0;
    Query query = new Query(nextConfig);
    //    System.out.println("new Query request: " + nextConfig);
    AMOCommand queryCmd = new AMOCommand(query, -1, address());
    broadcastToShardMasters(new PaxosRequest(queryCmd));
  }

  private void sendShards() {
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
        broadcast(move, destServers);
      }
    }
  }
}
