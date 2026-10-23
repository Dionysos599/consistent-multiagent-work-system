package cmws.paxos;

import cmws.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;
  private final AMOCommand command;
}

@Data
final class HeartbeatTimer implements Timer {
  static final int HEARTBEAT_MILLIS = 25;
}

@Data
final class CheckHeartbeatTimer implements Timer {
  static final int HEARTBEAT_CHECK_MILLIS = 100;
  private final Ballot ballot_num;
}

@Data
final class P1ATimer implements Timer {
  static final int P1A_RETRY_MILLIS = 25;
  private final P1A p1aRequest;
}

@Data
final class P2ATimer implements Timer {
  static final int P2A_RETRY_MILLIS = 50;
  private final P2A p2aRequest;
}
