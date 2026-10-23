package cmws.primarybackup;

import cmws.atmostonce.AMOApplication;
import cmws.atmostonce.AMOCommand;
import dslabs.framework.Application;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class PingCheckTimer implements Timer {
  static final int PING_CHECK_MILLIS = 100;
}

@Data
final class PingTimer implements Timer {
  static final int PING_MILLIS = 25;
}

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  private final AMOCommand amoCommand;
}

@Data
final class ForwardRequestTimer implements Timer {
  static final int FORWARD_RETRY_MILLIS = 50;

  private final AMOCommand amoCommand;
  private final int viewNum;
}

@Data
final class StateTransferRequestTimer implements Timer {
  static final int STATE_RETRY_MILLIS = 25;
  private final AMOApplication<Application> app;
  private final int viewNum;
}
