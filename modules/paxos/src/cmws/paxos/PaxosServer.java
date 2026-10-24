package cmws.paxos;

import cmws.atmostonce.AMOApplication;
import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import cmws.core.Effect;
import cmws.core.Send;
import cmws.core.SetTimer;
import cmws.shardmaster.ShardMaster.Query;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Adapts {@link PaxosCore} to the test framework's {@link Node}: every handler forwards its event
 * to the core and carries out the effects the core returns.
 *
 * <p>A server runs in one of two modes. With an {@link Application} it is a complete replicated
 * state machine and answers clients itself; with a parent address it is a sub-node whose chosen
 * commands are handed to the parent as {@link PaxosDecision}s (used by the shard store).
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  private final AMOApplication<Application> app;
  private final Address parentAddr;
  private final PaxosCore core;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.app = new AMOApplication<>(app);
    this.parentAddr = null;
    this.core = new PaxosCore(address, servers);
  }

  public PaxosServer(Address address, Address[] servers, Address parentAddr) {
    super(address);
    this.app = null;
    this.parentAddr = parentAddr;
    this.core = new PaxosCore(address, servers);
  }

  @Override
  public void init() {
    apply(core.start());
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}. Log slots are numbered starting with 1.
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    return core.status(logSlotNum);
  }

  /**
   * Return the command associated with a given slot in the server's local log, unwrapped from its
   * {@link AMOCommand}, or {@code null} if the slot is {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}.
   */
  public Command command(int logSlotNum) {
    return core.command(logSlotNum);
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log: the first slot which
   * has not yet been garbage-collected. By default, the first non-cleared slot is 1.
   */
  public int firstNonCleared() {
    return core.firstNonCleared();
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, or 0 if there are no
   * non-empty slots.
   */
  public int lastNonEmpty() {
    return core.lastNonEmpty();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    if (!core.isActive()) {
      return;
    }

    AMOCommand cmd = m.command();
    // single server
    if (core.groupSize() == 1) {
      if (app != null) {
        AMOResult result = app.execute(cmd);
        send(new PaxosReply(result), sender);
      } else {
        // Lab 4
        handleMessage(new PaxosDecision(cmd), parentAddr);
      }
      return;
    }

    // lab 4 special handling for Query
    if (app != null && cmd.command() instanceof Query) {
      Result result = app.application().execute(cmd.command());
      send(new PaxosReply(new AMOResult(cmd.sequenceNum(), result)), sender);
      return;
    }

    if (app != null && app.alreadyExecuted(cmd)) {
      // Send cached result
      AMOResult result = app.execute(cmd);
      send(new PaxosReply(result), sender);
      return;
    }

    apply(core.propose(cmd));
  }

  private void handleP1A(P1A m, Address sender) {
    apply(core.onP1A(m, sender));
  }

  private void handleP1B(P1B m, Address sender) {
    apply(core.onP1B(m, sender));
  }

  private void handleP2A(P2A m, Address sender) {
    apply(core.onP2A(m, sender));
  }

  private void handleP2B(P2B m, Address sender) {
    apply(core.onP2B(m, sender));
  }

  private void handleHeartbeatRequest(HeartbeatRequest m, Address sender) {
    apply(core.onHeartbeatRequest(m, sender));
  }

  private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
    apply(core.onHeartbeatReply(m, sender));
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onHeartbeatTimer(HeartbeatTimer t) {
    apply(core.onHeartbeatTimer(t));
  }

  private void onCheckHeartbeatTimer(CheckHeartbeatTimer t) {
    apply(core.onCheckHeartbeatTimer(t));
  }

  private void onP1ATimer(P1ATimer t) {
    apply(core.onP1ATimer(t));
  }

  private void onP2ATimer(P2ATimer t) {
    apply(core.onP2ATimer(t));
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
      } else if (effect instanceof Execute x) {
        execute(x);
      } else {
        throw new IllegalStateException("unknown effect " + effect);
      }
    }
  }

  private void execute(Execute x) {
    AMOCommand cmd = x.command();
    if (app != null) {
      AMOResult result = app.execute(cmd);
      if (x.respond() && cmd.sender() != null) {
        send(new PaxosReply(result), cmd.sender());
      }
    } else {
      handleMessage(new PaxosDecision(cmd), parentAddr);
    }
  }
}
