package cmws.shardkv;

import cmws.atmostonce.AMOApplication;
import cmws.atmostonce.AMOCommand;
import cmws.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import cmws.shardmaster.ShardMaster.ShardConfig;
import java.util.Map;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
  private final AMOCommand amoCommand;
  private final int configNum;
}

@Data
final class ShardStoreReply implements Message {
  private final AMOResult result;
  private final int configNum;
  private final boolean hasError;
}

@Data
final class ShardMove implements Message {
  private final int groupId;
  private final int configNum;
  private final Address[] senderGroup;
  private final Map<Integer, AMOApplication> shardToApp;
}

@Data
final class ShardMoveAck implements Message {
  private final int configNum;
  private final int groupId;
}

interface ShardStoreCommand extends Command {}

@Data
final class StartReconfiguration implements ShardStoreCommand {
  private final ShardConfig newConfig;
}

@Data
final class ProcessShardMove implements ShardStoreCommand {
  private final ShardMove shardMove;
}

@Data
final class ProcessShardMoveAck implements ShardStoreCommand {
  private final ShardMoveAck ack;
}
