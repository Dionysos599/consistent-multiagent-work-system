package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import java.io.Serializable;
import lombok.Data;

/** LogEntry represents a single slot in the Paxos log */
@Data
class LogEntry implements Serializable {
  private Ballot ballot;
  private PaxosLogSlotStatus status;
  private AMOCommand command;
}
