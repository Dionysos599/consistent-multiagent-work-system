package cmws.paxos;

import static cmws.paxos.CheckHeartbeatTimer.HEARTBEAT_CHECK_MILLIS;
import static cmws.paxos.HeartbeatTimer.HEARTBEAT_MILLIS;
import static cmws.paxos.P1ATimer.P1A_RETRY_MILLIS;
import static cmws.paxos.P2ATimer.P2A_RETRY_MILLIS;
import static cmws.paxos.PaxosLogSlotStatus.ACCEPTED;
import static cmws.paxos.PaxosLogSlotStatus.CHOSEN;
import static cmws.paxos.PaxosLogSlotStatus.CLEARED;
import static cmws.paxos.PaxosLogSlotStatus.EMPTY;

import cmws.atmostonce.AMOCommand;
import cmws.core.Effect;
import cmws.core.Send;
import cmws.core.SetTimer;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * The Multi-Paxos protocol as a pure state machine.
 *
 * <p>Every entry point takes an event and returns the {@link Effect}s it causes; the core never
 * sends a message, arms a timer or touches an application itself. {@link PaxosServer} adapts it to
 * the test framework's {@code Node}. The behaviour is the one described in the Paxos design
 * document: stable leader, heartbeats that also carry chosen log entries, log merging when a new
 * leader is elected, and garbage collection of slots every replica has executed.
 */
@ToString
@EqualsAndHashCode
final class PaxosCore implements Serializable {
  private final Address self;

  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

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

  PaxosCore(Address self, Address[] servers) {
    this.self = self;
    this.servers = servers;
    this.ballot_num = new Ballot(0, self);
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

  /* -----------------------------------------------------------------------------------------------
   *  Queries
   * ---------------------------------------------------------------------------------------------*/
  boolean isActive() {
    return active;
  }

  int groupSize() {
    return servers.length;
  }

  PaxosLogSlotStatus status(int logSlotNum) {
    if (logSlotNum < firstNonCleared) {
      return CLEARED;
    }
    if (log.isEmpty() || !log.containsKey(logSlotNum)) {
      return EMPTY;
    }
    LogEntry entry = log.get(logSlotNum);
    return entry != null ? entry.status() : EMPTY;
  }

  Command command(int logSlotNum) {
    PaxosLogSlotStatus status = status(logSlotNum);
    if (status == CLEARED || status == EMPTY) {
      return null;
    }
    LogEntry entry = log.get(logSlotNum);
    if (entry != null && entry.command() != null) {
      return entry.command().command(); // unwrap
    }
    return null;
  }

  int firstNonCleared() {
    return firstNonCleared;
  }

  int lastNonEmpty() {
    return firstEmpty - 1;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Events
   * ---------------------------------------------------------------------------------------------*/
  List<Effect> start() {
    List<Effect> out = new ArrayList<>();
    if (servers.length == 1) {
      active = true;
      return out;
    }
    startPhase1(out);
    out.add(new SetTimer(new CheckHeartbeatTimer(ballot_num), HEARTBEAT_CHECK_MILLIS));
    return out;
  }

  /** Propose a client command in the next free slot (leader only). */
  List<Effect> propose(AMOCommand cmd) {
    List<Effect> out = new ArrayList<>();
    if (!active) {
      return out;
    }

    // Already proposed and not yet executed: don't propose it a second time. A copy that has
    // already been executed must NOT suppress a retry. Callers that run on top of this log (the
    // shard store) resend commands that were decided too early to take effect, and treating the
    // old log entry as "still pending" turned such a retry into a permanent no-op; because slots
    // are only garbage-collected once every replica has executed them, a single dead replica kept
    // that entry around forever.
    if (!cmd.readOnly()
        && log.entrySet().stream()
            .anyMatch(e -> e.getKey() >= firstNotExec && cmd.equals(e.getValue().command()))) {
      return out;
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
    p2bResponses.get(slot).add(self); // Count own vote

    broadcastToServers(p2a, out);
    out.add(new SetTimer(new P2ATimer(p2a), P2A_RETRY_MILLIS));
    return out;
  }

  List<Effect> onP1A(P1A m, Address sender) {
    List<Effect> out = new ArrayList<>();
    handleP1A(m, sender, out);
    return out;
  }

  List<Effect> onP1B(P1B m, Address sender) {
    List<Effect> out = new ArrayList<>();
    if (!m.ballot().equals(ballot_num)) {
      return out;
    }
    if (active) {
      return out;
    }

    p1bResponses.add(sender);
    mergeLog(m.log());

    if (p1bResponses.size() > (servers.length / 2)) {
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
          p2bResponses.get(slot).add(self); // Count own vote

          broadcastToServers(p2a, out);
          out.add(new SetTimer(new P2ATimer(p2a), P2A_RETRY_MILLIS));
        } else if (log.get(slot).status() == ACCEPTED) {
          // Re-propose a value accepted under an older ballot, and keep retrying it: without the
          // timer a single lost message leaves the slot undecided and stalls execution forever.
          LogEntry accepted = log.get(slot);
          accepted.ballot(ballot_num);
          P2A p2a = new P2A(ballot_num, slot, accepted.command());

          p2bResponses.put(slot, new HashSet<>());
          p2bResponses.get(slot).add(self); // Count own vote

          broadcastToServers(p2a, out);
          out.add(new SetTimer(new P2ATimer(p2a), P2A_RETRY_MILLIS));
        }
      }

      // become leader, let everyone know
      broadcastToServers(new HeartbeatRequest(ballot_num, log, firstNonCleared), out);
      out.add(new SetTimer(new HeartbeatTimer(), HEARTBEAT_MILLIS));
    }
    return out;
  }

  List<Effect> onP2A(P2A m, Address sender) {
    List<Effect> out = new ArrayList<>();
    handleP2A(m, sender, out);
    return out;
  }

  List<Effect> onP2B(P2B m, Address sender) {
    List<Effect> out = new ArrayList<>();
    if (!m.ballot().equals(ballot_num) || !active) {
      return out;
    }

    isLeaderAlive = true;

    Set<Address> responses = p2bResponses.computeIfAbsent(m.slotNum(), k -> new HashSet<>());
    responses.add(sender);

    if (isMajority(responses.size())) {
      LogEntry entry = log.get(m.slotNum());
      if (entry != null) {
        entry.status(CHOSEN);
      }
      executeCommands(out);
    }
    return out;
  }

  List<Effect> onHeartbeatRequest(HeartbeatRequest m, Address sender) {
    List<Effect> out = new ArrayList<>();
    handleHeartbeatRequest(m, sender, out);
    return out;
  }

  List<Effect> onHeartbeatReply(HeartbeatReply m, Address sender) {
    List<Effect> out = new ArrayList<>();
    if (!m.ballot().equals(ballot_num) || !active) {
      return out;
    }

    garbageCollect.put(sender, m.firstNotExec());
    garbageCollect.putIfAbsent(self, firstNotExec);
    if (garbageCollect.size() == servers.length) {
      collectGarbage();
    }
    return out;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timers
   * ---------------------------------------------------------------------------------------------*/
  List<Effect> onHeartbeatTimer(HeartbeatTimer t) {
    List<Effect> out = new ArrayList<>();
    if (active) {
      // Only include CHOSEN entries to reduce message size
      Map<Integer, LogEntry> chosenLog = new HashMap<>();
      for (Map.Entry<Integer, LogEntry> entry : log.entrySet()) {
        if (entry.getValue().status() == CHOSEN) {
          chosenLog.put(entry.getKey(), entry.getValue());
        }
      }

      for (Address server : servers) {
        if (!server.equals(self)) {
          out.add(new Send(new HeartbeatRequest(ballot_num, chosenLog, firstNonCleared), server));
        }
      }

      out.add(new SetTimer(t, HEARTBEAT_MILLIS));
    }
    return out;
  }

  List<Effect> onCheckHeartbeatTimer(CheckHeartbeatTimer t) {
    List<Effect> out = new ArrayList<>();
    if (t.ballot_num().equals(ballot_num)) {
      if (!active && !isLeaderAlive) {
        startPhase1(out);
      }
    }
    isLeaderAlive = false;
    out.add(new SetTimer(new CheckHeartbeatTimer(ballot_num), HEARTBEAT_CHECK_MILLIS));
    return out;
  }

  List<Effect> onP1ATimer(P1ATimer t) {
    List<Effect> out = new ArrayList<>();
    // still attempting with same ballot
    if (!active && t.p1aRequest().ballot().equals(ballot_num) && !isLeaderAlive) {
      broadcastToServers(t.p1aRequest(), out);
      out.add(new SetTimer(t, P1A_RETRY_MILLIS));
    }
    return out;
  }

  List<Effect> onP2ATimer(P2ATimer t) {
    List<Effect> out = new ArrayList<>();
    // still leader with same ballot and slot not decided
    P2A p2a = t.p2aRequest();
    if (active && p2a.ballot().equals(ballot_num)) {
      LogEntry entry = log.get(p2a.slotNum());

      if (entry != null && entry.status() != CHOSEN) {
        broadcastToServers(p2a, out);
        out.add(new SetTimer(t, P2A_RETRY_MILLIS));
      }
    }
    return out;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message handling
   * ---------------------------------------------------------------------------------------------*/
  private void handleP1A(P1A m, Address sender, List<Effect> out) {
    if (m.ballot().compareTo(ballot_num) >= 0) {
      // new leader
      if (m.ballot().compareTo(ballot_num) > 0) {
        ballot_num = m.ballot();
        active = false;
        p1bResponses.clear();
        p2bResponses.clear();
        garbageCollect.clear();
        // The single CheckHeartbeatTimer chain started in start() keeps running; starting another
        // chain here made the check interval shrink with every ballot change.
      }
      isLeaderAlive = true;

      if (!sender.equals(self)) {
        out.add(new Send(new P1B(ballot_num, log), sender));
      }
    }
  }

  private void handleP2A(P2A m, Address sender, List<Effect> out) {
    if (m.ballot().compareTo(ballot_num) >= 0 && m.slotNum() >= firstNonCleared) {
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

      LogEntry existing = log.get(m.slotNum());
      if (existing != null && existing.status() == CHOSEN) {
        // Already decided: never downgrade a chosen slot, just acknowledge.
        if (!sender.equals(self)) {
          out.add(new Send(new P2B(ballot_num, m.slotNum()), sender));
        }
        return;
      }

      LogEntry entry = log.getOrDefault(m.slotNum(), new LogEntry());
      entry.ballot(m.ballot());
      entry.status(ACCEPTED);
      entry.command(m.command());
      log.put(m.slotNum(), entry);
      firstEmpty = Math.max(firstEmpty, m.slotNum() + 1);

      if (!sender.equals(self)) {
        out.add(new Send(new P2B(ballot_num, m.slotNum()), sender));
      }
    }
  }

  private void handleHeartbeatRequest(HeartbeatRequest m, Address sender, List<Effect> out) {
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
    for (Map.Entry<Integer, LogEntry> entry : m.log().entrySet()) {
      int slot = entry.getKey();
      LogEntry leaderEntry = entry.getValue();

      if (leaderEntry.status() != CHOSEN) {
        continue;
      }

      LogEntry myEntry = log.getOrDefault(slot, new LogEntry());
      if (myEntry.status() != CHOSEN && slot >= firstNonCleared) {
        myEntry.ballot(leaderEntry.ballot());
        myEntry.status(CHOSEN);
        myEntry.command(leaderEntry.command());
        log.put(slot, myEntry);
      }

      firstEmpty = Math.max(firstEmpty, slot + 1);
    }

    executeCommands(out);

    // Garbage collect up to leader's first non-cleared slot
    if (m.firstNotCleared() > firstNonCleared) {
      for (int slot = firstNonCleared; slot < m.firstNotCleared(); slot++) {
        log.remove(slot);
      }
      firstNonCleared = m.firstNotCleared();
    }

    // Also answers the leader's own locally delivered heartbeat, which seeds its GC bookkeeping.
    out.add(new Send(new HeartbeatReply(ballot_num, firstNotExec), sender));
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  /** Start the first phase of Paxos: called when this server decides to run for leader. */
  private void startPhase1(List<Effect> out) {
    ballot_num = new Ballot(ballot_num.round() + 1, self);
    active = false;
    p1bResponses.clear();
    p2bResponses.clear();
    garbageCollect.clear();
    temp_log = new HashMap<>(log); // Initialize temp_log with your current log

    // Count own vote
    p1bResponses.add(self);

    P1A p1a = new P1A(ballot_num);
    for (Address server : servers) {
      if (!server.equals(self)) {
        out.add(new Send(p1a, server));
      }
    }

    out.add(new SetTimer(new P1ATimer(p1a), P1A_RETRY_MILLIS));
  }

  /** Deliver {@code m} to this server's own handlers where applicable, then to all the others. */
  private void broadcastToServers(Message m, List<Effect> out) {
    if (m instanceof P1A) {
      handleP1A((P1A) m, self, out);
    } else if (m instanceof P2A) {
      handleP2A((P2A) m, self, out);
    } else if (m instanceof HeartbeatRequest) {
      handleHeartbeatRequest((HeartbeatRequest) m, self, out);
    }

    for (Address server : servers) {
      if (!server.equals(self)) {
        out.add(new Send(m, server));
      }
    }
  }

  private boolean isMajority(int count) {
    return count > (servers.length / 2);
  }

  /** Emit {@link Execute} for every chosen command that is next in log order. */
  private void executeCommands(List<Effect> out) {
    while (true) {
      LogEntry entry = log.get(firstNotExec);

      if (entry == null || entry.status() != CHOSEN) {
        break;
      }

      AMOCommand cmd = entry.command();
      if (cmd != null) { // Could be a no-op
        out.add(new Execute(cmd, active));
      }

      firstNotExec++;
    }
  }

  /** Merge the log entries from another server into the temporary log (used during election). */
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
      if (currentEntry != null && currentEntry.status() == CHOSEN) {
        continue; // a decided value is never replaced by an accepted one
      }
      if (currentEntry == null || (otherEntry.ballot().compareTo(currentEntry.ballot()) > 0)) {
        temp_log.put(slot, otherEntry);
      }
    }
  }

  /** Garbage collect slots every server has executed (needs a heartbeat reply from all). */
  private void collectGarbage() {
    if (garbageCollect.size() < servers.length) {
      return;
    }

    int minFirstNotExec = Integer.MAX_VALUE;
    for (int slot : garbageCollect.values()) {
      minFirstNotExec = Math.min(minFirstNotExec, slot);
    }

    if (minFirstNotExec > firstNonCleared) {
      for (int slot = firstNonCleared; slot < minFirstNotExec; slot++) {
        log.remove(slot);
      }
      firstNonCleared = minFirstNotExec;

      // Reset the garbage collection tracking for next round
      garbageCollect.clear();
      garbageCollect.put(self, firstNotExec);
    }
  }
}
