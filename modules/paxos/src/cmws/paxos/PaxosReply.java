package cmws.paxos;

import cmws.atmostonce.AMOResult;
import dslabs.framework.Message;
import lombok.Data;

@Data
public final class PaxosReply implements Message {
  private final AMOResult result;
}
