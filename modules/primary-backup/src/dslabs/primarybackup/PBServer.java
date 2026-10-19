package dslabs.primarybackup;

import static dslabs.primarybackup.ForwardRequestTimer.FORWARD_RETRY_MILLIS;
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.StateTransferRequestTimer.STATE_RETRY_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;
  private AMOApplication<Application> app;
  private View view;
  private boolean isTransferring;
  private AMOCommand pendingCommand;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;

    this.app = new AMOApplication<>(app);
    this.isTransferring = false;
    this.view = new View(STARTUP_VIEWNUM, null, null);
    this.pendingCommand = null;
  }

  @Override
  public void init() {
    send(new Ping(ViewServer.STARTUP_VIEWNUM), viewServer);
    set(new PingTimer(), PING_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleRequest(Request m, Address sender) {
    // We're the primary, state transfer is complete, no pendingCommand
    // then we can accept the new request
    if (view.primary() != null
        && pendingCommand == null
        && view.primary().equals(address())
        && !isTransferring) {
      pendingCommand = m.amoCommand();

      if (view.backup() != null) {
        if (app.alreadyExecuted(pendingCommand)) {
          AMOResult result = app.execute(m.amoCommand());
          send(new Reply(result), sender);
          pendingCommand = null;
          return;
        }
        send(new ForwardRequest(view.viewNum(), m.amoCommand()), view.backup());
        set(new ForwardRequestTimer(m.amoCommand(), view.viewNum()), FORWARD_RETRY_MILLIS);
      } else {
        AMOResult result = app.execute(m.amoCommand());
        send(new Reply(result), sender);
        pendingCommand = null;
      }
    }
  }

  private void handleViewReply(ViewReply m, Address sender) {
    if (view.viewNum() >= m.view().viewNum() || isTransferring) {
      return;
    }

    view = m.view();
    // idle server do nothing
    if (!Objects.equals(view.primary(), address()) && !Objects.equals(view.backup(), address())) {
      pendingCommand = null;
      return;
    }
    if (Objects.equals(view.primary(), address()) && view.backup() != null && !isTransferring) {
      isTransferring = true;
      send(new StateTransferRequest(view.viewNum(), app), view.backup());
      set(new StateTransferRequestTimer(app, view.viewNum()), STATE_RETRY_MILLIS);
      return;
    }

    if (Objects.equals(view.backup(), address()) && !isTransferring) {
      pendingCommand = null;
      isTransferring = true;
      return;
    }
  }

  private void handleForwardRequest(ForwardRequest m, Address sender) {
    if (!isTransferring
        && Objects.equals(view.primary(), sender)
        && Objects.equals(view.backup(), address())
        && view.viewNum() == m.viewNum()) {

      app.execute(m.amoCommand());
      send(new ForwardReply(view.viewNum(), m.amoCommand()), sender);
    }
  }

  private void handleForwardReply(ForwardReply m, Address sender) {
    if (!isTransferring
        && Objects.equals(view.primary(), address())
        && Objects.equals(view.backup(), sender)
        && pendingCommand != null
        && view.viewNum() == m.viewNum()) {

      if (Objects.equals(m.amoCommand(), pendingCommand)) {
        AMOResult result = app.execute(pendingCommand);
        send(new Reply(result), pendingCommand.sender());
        pendingCommand = null;
      }
    }
  }

  private void handleStateTransferRequest(StateTransferRequest m, Address sender) {
    if (Objects.equals(view.primary(), sender)
        && Objects.equals(view.backup(), address())
        && (m.viewNum() == view.viewNum())
        && isTransferring) {

      this.app = m.app();
      isTransferring = false;
      send(new StateTransferReply(view.viewNum()), sender);
    }
    if (view.viewNum() >= m.viewNum()) {
      send(new StateTransferReply(view.viewNum()), sender);
    }
  }

  private void handleStateTransferReply(StateTransferReply m, Address sender) {
    if (isTransferring
        && Objects.equals(view.primary(), address())
        && Objects.equals(view.backup(), sender)
        && m.viewNum() == view.viewNum()) {

      isTransferring = false;
      pendingCommand = null;
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    // Don't ping VS during state transfer
    if (!isTransferring) {
      send(new Ping(view.viewNum()), viewServer);
    }
    set(new PingTimer(), PING_MILLIS);
  }

  private void onForwardRequestTimer(ForwardRequestTimer t) {
    if (t.amoCommand().equals(pendingCommand)
        && view.primary().equals(address())
        && pendingCommand != null
        && view.backup() != null) {
      if (!isTransferring) {
        send(new ForwardRequest(view.viewNum(), t.amoCommand()), view.backup());
      }
      set(t, FORWARD_RETRY_MILLIS);
    }
  }

  private void onStateTransferRequestTimer(StateTransferRequestTimer t) {
    // State transfer is not complete and we're primary with a backup
    if (isTransferring && (t.viewNum() == view.viewNum())) {

      send(new StateTransferRequest(view.viewNum(), app), view.backup());
      set(t, STATE_RETRY_MILLIS);
    }
  }
}
