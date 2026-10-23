package cmws.paxos;

import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;
  private int seqNum;
  private AMOCommand pendingCommand;
  private AMOResult pendingResult;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    pendingCommand = new AMOCommand(command, seqNum, address());
    pendingResult = null;

    // Broadcast request
    broadcast(new PaxosRequest(pendingCommand), servers);
    set(new ClientTimer(pendingCommand), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    return pendingResult != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Wait until we have a result
    while (pendingResult == null) {
      this.wait();
    }
    return pendingResult.result();
  }

  /* -----------------------------------------------------------------------------------------------
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    if (pendingCommand != null && (m.result().sequenceNum() == pendingCommand.sequenceNum())) {
      pendingResult = m.result();
      seqNum++;
      this.notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (pendingCommand != null
        && pendingResult == null
        && Objects.equals(t.command(), pendingCommand)) { // Check if the command is still pending

      // Broadcast request
      broadcast(new PaxosRequest(pendingCommand), servers);
      set(new ClientTimer(pendingCommand), ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }
}
