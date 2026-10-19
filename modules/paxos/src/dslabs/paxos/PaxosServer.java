package dslabs.paxos;

import static dslabs.paxos.CheckHeartbeatTimer.HEARTBEAT_CHECK_MILLIS;
import static dslabs.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static dslabs.paxos.P1ATimer.P1A_RETRY_MILLIS;
import static dslabs.paxos.P2ATimer.P2A_RETRY_MILLIS;
import static dslabs.paxos.PaxosLogSlotStatus.*;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Node;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  private final AMOApplication<Application> app;
  private Ballot ballot_num;
  private Map<Integer, LogEntry> log;
  private Map<Integer, LogEntry> temp_log;
  private boolean active;
  private int firstNotExec; // slot_out
  private int firstEmpty; // slot_in
  private int firstNonCleared;
  private boolean isLeaderAlive;
  private final Set<Address> p1bResponses;
  private final Map<Integer, Set<Address>> p2bResponses;
  private final Map<Address, Integer> garbageCollect;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;
    this.app = new AMOApplication<>(app);
    this.ballot_num = new Ballot(0, address);
    this.log = new HashMap<>();
    this.temp_log = new HashMap<>();
    this.active = false;
    this.firstNotExec = 1;
    this.firstEmpty = 1;
    this.firstNonCleared = 1;
    this.isLeaderAlive = false;
    this.p1bResponses = new HashSet<>();
    this.p2bResponses = new HashMap<>();
    this.garbageCollect = new HashMap<>();
  }

  @Override
  public void init() {
    if (servers.length == 1) {
      active = true;
      return;
    } else {
      startPhase1();
    }

    set(new CheckHeartbeatTimer(ballot_num), HEARTBEAT_CHECK_MILLIS);
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
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    if (logSlotNum < firstNonCleared()) {
      return CLEARED;
    }
    if (this.log.isEmpty() || (!this.log.containsKey(logSlotNum))) {
      return EMPTY;
    }

    LogEntry entry = log.get(logSlotNum);
    return entry != null ? entry.status() : EMPTY;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    PaxosLogSlotStatus status = status(logSlotNum);
    if (status == CLEARED || status == EMPTY) {
      return null;
    }

    LogEntry entry = log.get(logSlotNum);
    if (entry != null && entry.command() != null) {
      AMOCommand amoCommand = entry.command();
      return amoCommand.command(); // unwrap
    }

    return null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    return firstNonCleared;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    return firstEmpty - 1;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    if (!active) {
      return;
    }

    AMOCommand cmd = m.command();
    // single server
    if (servers.length == 1) {
      AMOResult result = app.execute(cmd);
      send(new PaxosReply(result), sender);
      return;
    }

    if (app.alreadyExecuted(cmd)) {
      // Send cached result
      AMOResult result = app.execute(cmd);
      send(new PaxosReply(result), sender);
      return;
    }

    // already proposed but haven't chosen to execute
    for (int i = firstNotExec; i < firstEmpty; i++) {
      if (log.get(i) != null && Objects.equals(log.get(i).command(), cmd)) {
        return;
      }
    }

    int slot = firstEmpty++;

    LogEntry entry = new LogEntry();
    entry.ballot(ballot_num);
    entry.status(ACCEPTED);
    entry.command(cmd);
    log.put(slot, entry);

    // Phase 2
    P2A p2a = new P2A(ballot_num, slot, cmd);
    p2bResponses.put(slot, new HashSet<>());
    p2bResponses.get(slot).add(address()); // Count own vote

    broadcastToServers(p2a);
    set(new P2ATimer(p2a), P2A_RETRY_MILLIS);
  }

  private void handleP1A(P1A m, Address sender) {
    if (m.ballot().compareTo(ballot_num) >= 0) {
      // new leader
      if (m.ballot().compareTo(ballot_num) > 0) {
        ballot_num = m.ballot();
        //        isLeaderAlive = false;
        active = false;
        p1bResponses.clear();
        p2bResponses.clear();
        garbageCollect.clear();
        set(new CheckHeartbeatTimer(ballot_num), HEARTBEAT_CHECK_MILLIS);
      }
      isLeaderAlive = true;

      if (sender != address()) {
        send(new P1B(ballot_num, log), sender);
      }
    }
  }

  private void handleP1B(P1B m, Address sender) {
    if (!m.ballot().equals(ballot_num)) {
      return;
    }

    if (active) {
      return;
    }

    p1bResponses.add(sender);
    mergeLog(m.log());

    if (p1bResponses.size() > (servers.length / 2)) {
      //      System.out.println("New Leader: " + address());
      //      System.out.println("firstEmpty: " + firstEmpty);
      active = true;
      for (int slot : temp_log.keySet()) {
        firstEmpty = Math.max(firstEmpty, slot + 1);
      }
      log = new HashMap<>(temp_log);
      temp_log.clear();

      // Fill any holes with no-ops
      for (int slot = firstNotExec; slot < firstEmpty; slot++) {
        if (!log.containsKey(slot) || log.get(slot).status() == EMPTY) {
          LogEntry entry = new LogEntry();
          entry.ballot(ballot_num);
          entry.status(ACCEPTED);
          entry.command(null); // no-op
          log.put(slot, entry);

          // Phase 2
          P2A p2a = new P2A(ballot_num, slot, null);

          p2bResponses.put(slot, new HashSet<>());
          p2bResponses.get(slot).add(address()); // Count own vote

          broadcastToServers(p2a);
          set(new P2ATimer(p2a), P2A_RETRY_MILLIS);
        } else if (log.containsKey(slot) && log.get(slot).status() == ACCEPTED) {
          P2A p2a = new P2A(ballot_num, slot, log.get(slot).command());

          p2bResponses.put(slot, new HashSet<>());
          p2bResponses.get(slot).add(address()); // Count own vote

          // Send to others
          broadcastToServers(p2a);
        }
      }

      // become leader, let everyone know
      HeartbeatRequest HBRequest = new HeartbeatRequest(ballot_num, log, firstNonCleared());
      broadcastToServers(HBRequest);
      set(new HeartbeatTimer(), HEARTBEAT_MILLIS);
    }
  }

  private void handleP2A(P2A m, Address sender) {
    if (m.ballot().compareTo(ballot_num) >= 0 && m.slotNum() >= firstNonCleared()) {
      if (!m.ballot().equals(ballot_num)) {
        // If strictly higher ballot, update and step down if leader
        ballot_num = m.ballot();
        if (active) {
          active = false;
          p1bResponses.clear();
          p2bResponses.clear();
          garbageCollect.clear();
        }
      }

      isLeaderAlive = true;

      LogEntry entry = log.getOrDefault(m.slotNum(), new LogEntry());
      entry.ballot(m.ballot());
      entry.status(ACCEPTED);
      entry.command(m.command());
      log.put(m.slotNum(), entry);
      firstEmpty = Math.max(firstEmpty, m.slotNum() + 1);

      if (sender != address()) {
        send(new P2B(ballot_num, m.slotNum()), sender);
      }
    }
  }

  private void handleP2B(P2B m, Address sender) {
    if (!m.ballot().equals(ballot_num) || !active) {
      return;
    }

    if (m.ballot().compareTo(ballot_num) >= 0) {
      isLeaderAlive = true;
    }

    Set<Address> responses = p2bResponses.computeIfAbsent(m.slotNum(), k -> new HashSet<>());
    responses.add(sender);

    if (isMajority(responses.size())) {
      // Mark slot as chosen
      LogEntry entry = log.get(m.slotNum());
      if (entry != null) {
        entry.status(CHOSEN);
      }

      executeCommands();
    }
  }

  private void handleHeartbeatRequest(HeartbeatRequest m, Address sender) {
    if (m.ballot().compareTo(ballot_num) < 0) {
      return;
    }

    if (m.ballot().compareTo(ballot_num) > 0) {
      ballot_num = m.ballot();
      if (active) {
        active = false;
        p1bResponses.clear();
        p2bResponses.clear();
        garbageCollect.clear();
      }
    }

    isLeaderAlive = true;

    // Update log
    //    boolean logUpdated = false;
    for (Map.Entry<Integer, LogEntry> entry : m.log().entrySet()) {
      int slot = entry.getKey();
      LogEntry leaderEntry = entry.getValue();

      if (leaderEntry.status() != CHOSEN) {
        continue;
      }

      LogEntry myEntry = log.getOrDefault(slot, new LogEntry());
      if (myEntry.status() != CHOSEN && slot >= firstNonCleared()) {
        myEntry.ballot(leaderEntry.ballot());
        myEntry.status(CHOSEN);
        myEntry.command(leaderEntry.command());
        log.put(slot, myEntry);
        //        logUpdated = true;
      }

      firstEmpty = Math.max(firstEmpty, slot + 1);
    }

    executeCommands();

    //    System.out.println("before garbage collect: " + log);
    //    System.out.println("local firstNotClear: " + firstNonCleared());
    //    System.out.println("firstNoneClear: " + m.firstNotCleared());

    // Garbage collect up to leader's first non-cleared slot
    if (m.firstNotCleared() > firstNonCleared) {
      for (int slot = firstNonCleared; slot < m.firstNotCleared(); slot++) {
        log.remove(slot);
      }
      firstNonCleared = m.firstNotCleared();
    }

    //    System.out.println("after garbage collect: " + log);

    send(new HeartbeatReply(ballot_num, firstNotExec), sender);
  }

  private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
    if (!m.ballot().equals(ballot_num) || !active) {
      return;
    }

    garbageCollect.put(sender, m.firstNotExec());
    garbageCollect.putIfAbsent(address(), firstNotExec);
    if (garbageCollect.size() == servers.length) {
      collectGarbage();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onHeartbeatTimer(HeartbeatTimer t) {
    if (active) {
      // Only include CHOSEN entries to reduce message size
      Map<Integer, LogEntry> chosenLog = new HashMap<>();
      for (Map.Entry<Integer, LogEntry> entry : log.entrySet()) {
        if (entry.getValue().status() == CHOSEN) {
          chosenLog.put(entry.getKey(), entry.getValue());
        }
      }

      for (Address server : servers) {
        if (!server.equals(address())) {
          send(new HeartbeatRequest(ballot_num, chosenLog, firstNonCleared()), server);
        }
      }

      set(new HeartbeatTimer(), HEARTBEAT_MILLIS);
    }
  }

  private void onCheckHeartbeatTimer(CheckHeartbeatTimer t) {
    if (t.ballot_num().equals(ballot_num)) {
      if (!active && !isLeaderAlive) {
        startPhase1();
      }
    }
    isLeaderAlive = false;
    set(new CheckHeartbeatTimer(ballot_num), HEARTBEAT_CHECK_MILLIS);
  }

  private void onP1ATimer(P1ATimer t) {
    // still attempting with same ballot
    if (!active && t.p1aRequest().ballot().equals(ballot_num) && !isLeaderAlive) {
      broadcastToServers(t.p1aRequest());
      set(t, P1A_RETRY_MILLIS);
    }
  }

  private void onP2ATimer(P2ATimer t) {
    // still leader with same ballot and slot not decided
    P2A p2a = t.p2aRequest();
    if (active && p2a.ballot().equals(ballot_num)) {
      LogEntry entry = log.get(p2a.slotNum());

      if (entry != null && entry.status() != CHOSEN) {
        broadcastToServers(p2a);
        set(t, P2A_RETRY_MILLIS);
      }
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  /**
   * Start the first phase of Paxos.
   *
   * <p>This method is called when the server is not active and needs to start a new election.
   */
  private void startPhase1() {
    ballot_num = new Ballot(ballot_num.round() + 1, address());
    active = false;
    p1bResponses.clear();
    p2bResponses.clear();
    garbageCollect.clear();
    temp_log = new HashMap<>(log); // Initialize temp_log with your current log

    // Count own vote
    p1bResponses.add(address()); // Add self to voters

    P1A p1a = new P1A(ballot_num);
    for (Address server : servers) {
      if (!server.equals(address())) {
        send(p1a, server);
      }
    }

    set(new P1ATimer(p1a), P1A_RETRY_MILLIS);
  }

  /**
   * Broadcast a message to all servers in the Paxos group.
   *
   * @param m the message to broadcast
   */
  private void broadcastToServers(Message m) {
    if (m instanceof P1A) {
      handleP1A((P1A) m, address());
    } else if (m instanceof P2A) {
      handleP2A((P2A) m, address());
    } else if (m instanceof HeartbeatRequest) {
      handleHeartbeatRequest((HeartbeatRequest) m, address());
    }

    for (Address server : servers) {
      if (!server.equals(address())) {
        send(m, server);
      }
    }
  }

  /**
   * Check if the count is a majority of the servers.
   *
   * @param count the number of responses
   * @return true if the count is a majority, false otherwise
   */
  private boolean isMajority(int count) {
    return count > (servers.length / 2);
  }

  /**
   * Execute commands in the log that have been chosen.
   *
   * <p>This method should be called after a command has been chosen to execute it and send the
   * result back to the client.
   */
  private void executeCommands() {
    while (true) {
      LogEntry entry = log.get(firstNotExec);

      if (entry == null || entry.status() != CHOSEN) {
        break;
      }

      AMOCommand cmd = entry.command();
      if (cmd != null) { // Could be a no-op
        AMOResult result = app.execute(cmd);

        if (active && cmd.sender() != null) {
          send(new PaxosReply(result), cmd.sender());
        }
      }

      firstNotExec++;
    }
  }

  /**
   * Merge the log entries from another server into the temporary log.
   *
   * <p>This method is used to merge logs during the leader election process.
   *
   * @param otherLog the log entries from another server
   */
  private void mergeLog(Map<Integer, LogEntry> otherLog) {
    for (Map.Entry<Integer, LogEntry> entry : otherLog.entrySet()) {
      int slot = entry.getKey();
      LogEntry otherEntry = entry.getValue();

      if (otherEntry.status() == EMPTY) {
        continue;
      }

      if (otherEntry.status() == CHOSEN || otherEntry.status() == CLEARED) {
        temp_log.put(slot, otherEntry);
        continue;
      }

      LogEntry currentEntry = temp_log.get(slot);
      if (currentEntry == null || (otherEntry.ballot().compareTo(currentEntry.ballot()) > 0)) {
        temp_log.put(slot, otherEntry);
      }
    }

    //    for (int slot : temp_log.keySet()) {
    //      firstEmpty = Math.max(firstEmpty, slot + 1);
    //    }
  }

  /**
   * Garbage collect slots that are no longer needed.
   *
   * <p>This method is called when the server has received heartbeat replies from all servers.
   */
  private void collectGarbage() {
    if (garbageCollect.size() < servers.length) {
      return; // Don't GC unless we've heard from all servers
    }

    int minFirstNotExec = Integer.MAX_VALUE;
    for (int slot : garbageCollect.values()) {
      minFirstNotExec = Math.min(minFirstNotExec, slot);
    }

    // Only GC slots that all servers have executed
    if (minFirstNotExec > firstNonCleared) {
      for (int slot = firstNonCleared; slot < minFirstNotExec; slot++) {
        log.remove(slot);
      }
      // Update slotGarbageCollection to track the new first non-cleared slot
      firstNonCleared = minFirstNotExec;

      // Reset the garbage collection tracking for next round
      garbageCollect.clear();
      garbageCollect.put(address(), firstNotExec);
    }
  }
}
