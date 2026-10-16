package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  // Your code here...
  private View currentView;
  private List<Address> idleAddr;
  private boolean isAck;
  private boolean isPrimaryAlive;
  private boolean isBackupAlive;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    this.currentView = new View(STARTUP_VIEWNUM, null, null);
    this.idleAddr = new ArrayList<>();
    this.isAck = false;
    this.isPrimaryAlive = false;
    this.isBackupAlive = false;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // First server that ping becomes the primary instantly
    if (this.currentView.viewNum() == STARTUP_VIEWNUM) {
      this.currentView = new View(INITIAL_VIEWNUM, sender, null);
      this.isPrimaryAlive = true;
      this.send(new ViewReply(this.currentView), sender);
      return;
    }
    // Primary acknowledge
    if (this.currentView.primary().equals(sender) && m.viewNum() == this.currentView.viewNum()) {
      this.isAck = true;
      this.isPrimaryAlive = true;
      // If no backup for current view and have idle server, pick the first idle to be backup
      // Update view if it has idle, else do nothing
      if (this.currentView.backup() == null && !this.idleAddr.isEmpty()) {
        this.isBackupAlive = true;
        this.isAck = false;
        this.currentView =
            new View(
                this.currentView.viewNum() + 1, this.currentView.primary(), this.idleAddr.get(0));
      }
    }
    // If the current view is ack, and we don't have backup, promote the first idle to be backup
    if (!this.currentView.primary().equals(sender)
        && this.currentView.backup() == null
        && this.isAck) {
      this.isBackupAlive = true;
      this.isAck = false;
      this.currentView =
          new View(this.currentView.viewNum() + 1, this.currentView.primary(), sender);
    }

    // If the current view is not ack, and we don't have backup, store the idle server
    if (!this.currentView.primary().equals(sender)
        && this.currentView.backup() == null
        && !this.isAck) {
      this.idleAddr.add(sender);
      // New idle server pings but we have primary and backup
    } else if (!this.currentView.primary().equals(sender)
        && this.currentView.backup() != null
        && !this.currentView.backup().equals(sender)) {
      this.idleAddr.add(sender);
    }

    // Backup server ping
    if (this.currentView.backup() != null && this.currentView.backup().equals(sender)) {
      this.isBackupAlive = true;
    }

    // Reply with current view
    this.send(new ViewReply(this.currentView), sender);
  }

  private void handleGetView(GetView m, Address sender) {
    // Your code here...
    this.send(new ViewReply(this.currentView), sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    // If primary dead and backup alive
    if (!this.isPrimaryAlive && this.isBackupAlive && !this.idleAddr.isEmpty() && this.isAck) {
      this.currentView =
          new View(this.currentView.viewNum() + 1, this.currentView.backup(), this.idleAddr.get(0));
      this.isAck = false;
    } else if (!this.isPrimaryAlive && this.isBackupAlive && this.isAck) {
      this.currentView = new View(this.currentView.viewNum() + 1, this.currentView.backup(), null);
      this.isAck = false;
    }

    // If backup dead
    if (this.isPrimaryAlive
        && this.currentView.backup() != null
        && !this.isBackupAlive
        && !this.idleAddr.isEmpty()
        && this.isAck) {
      this.currentView =
          new View(
              this.currentView.viewNum() + 1, this.currentView.primary(), this.idleAddr.get(0));
      this.isAck = false;
    } else if (this.isPrimaryAlive
        && this.currentView.backup() != null
        && !this.isBackupAlive
        && this.isAck) {
      this.currentView = new View(this.currentView.viewNum() + 1, this.currentView.primary(), null);
      this.isAck = false;
    }

    // Reset all value
    this.isPrimaryAlive = false;
    this.isBackupAlive = false;
    this.idleAddr = new ArrayList<>();
    set(t, PING_CHECK_MILLIS);
  }
}
