package cmws.shardkv;

import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import cmws.kvstore.KVStore.SingleKeyCommand;
import cmws.paxos.PaxosReply;
import cmws.paxos.PaxosRequest;
import cmws.shardmaster.ShardMaster.Query;
import cmws.shardmaster.ShardMaster.ShardConfig;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
  private ShardConfig shardConfig;
  private int seqNum;
  private AMOCommand pendingCommand;
  private AMOResult pendingResult;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ShardStoreClient(Address address, Address[] shardMasters, int numShards) {
    super(address, shardMasters, numShards);
    this.seqNum = 0;
    this.shardConfig = null;
    this.pendingResult = null;
    this.pendingCommand = null;
  }

  @Override
  public synchronized void init() {
    queryForConfiguration();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    pendingResult = null;
    pendingCommand = new AMOCommand(command, seqNum, address());

    if (shardConfig == null || shardConfig.configNum() < 0) {
      queryForConfiguration();
    } else {
      if (command instanceof SingleKeyCommand) {
        SingleKeyCommand skc = (SingleKeyCommand) command;
        String key = skc.key();
        int shard = keyToShard(key);

        Integer targetGroupId = null;
        Set<Address> targetServers = null;

        for (var entry : shardConfig.groupInfo().entrySet()) {
          if (entry.getValue().getRight().contains(shard)) {
            targetGroupId = entry.getKey();
            targetServers = entry.getValue().getLeft();
            break;
          }
        }
        if (targetGroupId != null && targetServers != null) {
          ShardStoreRequest request =
              new ShardStoreRequest(pendingCommand, shardConfig.configNum());
          broadcast(request, targetServers);
        }
      }
    }
    set(new ClientTimer(pendingCommand), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    return pendingResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    while (pendingResult == null) {
      this.wait();
    }
    return pendingResult.result();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
    if (m.hasError()) {
      // Server can't handle the request, refresh configuration
      // System.err.println("Client " + address() + " received error from server " + sender);
      queryForConfiguration();

      if (shardConfig != null && m.configNum() > shardConfig.configNum()) {
        resendCurrentCommand();
      }
      return;
    }

    if (pendingCommand != null && m.result().sequenceNum() == pendingCommand.sequenceNum()) {
      pendingResult = m.result();
      seqNum++;
      this.notify();
    }
  }

  private void resendCurrentCommand() {
    if (pendingCommand != null && shardConfig != null) {
      if (pendingCommand.command() instanceof SingleKeyCommand) {
        SingleKeyCommand skc = (SingleKeyCommand) pendingCommand.command();
        String key = skc.key();
        int shard = keyToShard(key);

        for (var entry : shardConfig.groupInfo().entrySet()) {
          if (entry.getValue().getRight().contains(shard)) {
            Set<Address> targetServers = entry.getValue().getLeft();
            ShardStoreRequest request =
                new ShardStoreRequest(pendingCommand, shardConfig.configNum());
            broadcast(request, targetServers);
            break;
          }
        }
      }
    }
  }

  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    Result result = m.result().result();
    if (result instanceof ShardConfig) {
      ShardConfig newConfig = (ShardConfig) result;
      // System.err.println("Client " + address() + " received config " + newConfig.configNum() + "
      // from " + sender);
      if (shardConfig == null || newConfig.configNum() > shardConfig.configNum()) {
        shardConfig = newConfig;
      }
    }
    // If Error, timer will retry automatically
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (pendingCommand != null && pendingResult == null) {
      if (t.command().equals(pendingCommand)) {
        queryForConfiguration();

        resendCurrentCommand();

        set(new ClientTimer(pendingCommand), ClientTimer.CLIENT_RETRY_MILLIS);
      }
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  private void queryForConfiguration() {
    // System.err.println("Client " + address() + " querying for configuration, seqNum=" +
    // configSeqNum);
    // Use new sequence number each time to avoid AMO caching stale results
    Query query = new Query(-1);
    AMOCommand queryCmd = new AMOCommand(query, -1, address());
    broadcastToShardMasters(new PaxosRequest(queryCmd));
  }
}
