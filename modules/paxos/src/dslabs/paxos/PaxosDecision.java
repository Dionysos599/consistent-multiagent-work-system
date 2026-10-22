package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;

@Data
public class PaxosDecision implements Message {
  private final AMOCommand command;
}
