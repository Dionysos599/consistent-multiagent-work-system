package cmws.paxos;

import cmws.atmostonce.AMOCommand;
import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;

@Data
class P1A implements Message {
  private final Ballot ballot;
}

@Data
class P1B implements Message {
  private final Ballot ballot;
  private final Map<Integer, LogEntry> log;
}

@Data
class P2A implements Message {
  private final Ballot ballot;
  private final int slotNum;
  private final AMOCommand command;
}

@Data
class P2B implements Message {
  private final Ballot ballot;
  private final int slotNum;
}

@Data
class HeartbeatRequest implements Message {
  private final Ballot ballot;
  private final Map<Integer, LogEntry> log;
  private final int firstNotCleared;
}

@Data
class HeartbeatReply implements Message {
  private final Ballot ballot;
  private final int firstNotExec; // slot_out
}
