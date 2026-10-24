package cmws.shardkv;

import cmws.core.Effect;
import cmws.core.Send;
import cmws.core.SetTimer;
import cmws.paxos.PaxosDecision;
import cmws.paxos.PaxosReply;
import cmws.paxos.PaxosRequest;
import cmws.paxos.PaxosServer;
import dslabs.framework.Address;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Adapts {@link ShardStoreCore} to the test framework's {@code Node}. The server owns a Paxos
 * sub-node for its group; the core's {@link Propose} effects are handed to it and its decisions come
 * back as {@link PaxosDecision}s.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private static final String PAXOS_ADDRESS_ID = "paxos";

  private final ShardStoreCore core;
  private Address paxosAddress;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  ShardStoreServer(
      Address address, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    super(address, shardMasters, numShards);
    this.core = new ShardStoreCore(address, shardMasters, numShards, group, groupId);
  }

  @Override
  public void init() {
    paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);

    Address[] group = core.group();
    Address[] paxosAddresses = new Address[group.length];
    for (int i = 0; i < paxosAddresses.length; i++) {
      paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
    }

    PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
    addSubNode(paxosServer);
    paxosServer.init();

    apply(core.start());
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    apply(core.onClientRequest(m, sender));
  }

  // Handle reply from ShardMaster
  private void handlePaxosReply(PaxosReply m, Address sender) {
    apply(core.onConfigReply(m.result().result()));
  }

  private void handleShardMove(ShardMove m, Address sender) {
    apply(core.onShardMove(m));
  }

  private void handleShardMoveAck(ShardMoveAck m, Address sender) {
    apply(core.onShardMoveAck(m));
  }

  // Handle decisions from Paxos
  private void handlePaxosDecision(PaxosDecision m, Address sender) {
    apply(core.onDecision(m.command()));
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onQueryTimer(QueryTimer t) {
    apply(core.onQueryTimer(t));
  }

  private void onShardMoveTimer(ShardMoveTimer t) {
    apply(core.onShardMoveTimer(t));
  }

  /* -----------------------------------------------------------------------------------------------
   *  Effects
   * ---------------------------------------------------------------------------------------------*/
  private void apply(List<Effect> effects) {
    for (Effect effect : effects) {
      if (effect instanceof Send s) {
        send(s.message(), s.to());
      } else if (effect instanceof SetTimer t) {
        set(t.timer(), t.millis());
      } else if (effect instanceof Propose p) {
        handleMessage(new PaxosRequest(p.command()), paxosAddress);
      } else {
        throw new IllegalStateException("unknown effect " + effect);
      }
    }
  }
}
