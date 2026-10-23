package cmws.paxos;

import cmws.atmostonce.AMOCommand;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosRequest implements Message {
  private final AMOCommand command;
}
