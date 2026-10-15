package dslabs.clientserver;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * <p>See the documentation of {@link Client} and {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
  private final Address serverAddress;
  private int seqNum = 0;
  private Command currentCommand;
  private AMOCommand amoCommand;
  private Result result;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleClient(Address address, Address serverAddress) {
    super(address);
    this.serverAddress = serverAddress;
  }

  @Override
  public synchronized void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    this.currentCommand = command;
    this.result = null;

    amoCommand = new AMOCommand(address(), seqNum, command);

    send(new Request(amoCommand), serverAddress);
    set(new ClientTimer(amoCommand), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    return result != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    while (result == null) {
      wait();
    }

    return result;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    AMOResult amoResult = m.result();

    if (amoCommand != null && amoResult.sequenceNum() == seqNum) {
      result = amoResult.result();
      seqNum++;

      notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    if (t.command().equals(amoCommand) && result == null) {
      send(new Request(amoCommand), serverAddress);
      set(new ClientTimer(amoCommand), ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }
}
