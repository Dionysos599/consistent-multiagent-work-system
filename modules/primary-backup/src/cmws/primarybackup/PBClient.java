package cmws.primarybackup;

import static cmws.primarybackup.ViewServer.STARTUP_VIEWNUM;

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
class PBClient extends Node implements Client {

  private final Address viewServer;
  private View view;
  private int seqNum;
  private AMOCommand pendingCommand;
  private AMOResult pendingResult;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
    this.seqNum = 0;
    this.view = new View(STARTUP_VIEWNUM, null, null);
    this.pendingResult = null;
    this.pendingCommand = null;
  }

  @Override
  public synchronized void init() {
    this.send(new GetView(), viewServer);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    pendingCommand = new AMOCommand(command, seqNum, address());
    pendingResult = null;

    // If no view or no primary, getView
    // If view cached, send request to primary
    if (view.primary() != null) {
      send(new Request(pendingCommand), view.primary());
    } else {
      send(new GetView(), viewServer);
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
  private synchronized void handleReply(Reply m, Address sender) {
    if (Objects.equals(m.amoResult().sequenceNum(), pendingCommand.sequenceNum())) {
      pendingResult = m.amoResult();
      seqNum++;
      this.notify();
    }
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    if (view.viewNum() < m.view().viewNum()) {
      view = m.view();
    }
    // Resend pending command to the new primary
    if (pendingResult == null && pendingCommand != null && view.primary() != null) {
      send(new Request(pendingCommand), view.primary());
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (pendingResult == null
        && pendingCommand != null
        && Objects.equals(t.amoCommand(), pendingCommand)) {
      send(new GetView(), viewServer);
      if (pendingResult == null && pendingCommand != null && view.primary() != null) {
        send(new Request(pendingCommand), view.primary());
      }
      set(t, ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }

}
