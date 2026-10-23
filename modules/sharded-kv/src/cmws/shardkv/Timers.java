package cmws.shardkv;

import cmws.atmostonce.AMOCommand;
import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 50;
  private final AMOCommand command;
}

@Data
final class QueryTimer implements Timer {
  static final int QUERY_RETRY_MILLIS = 25;
}

@Data
final class ShardMoveTimer implements Timer {
  static final int MOVE_RETRY_MILLIS = 25;
  private final int configNum;
}
