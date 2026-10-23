package cmws.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
  public static final int INITIAL_CONFIG_NUM = 0;

  private final int numShards;
  private final Map<Integer, ShardConfig> configs = new HashMap<>();
  private int currentConfigNum = -1;

  public ShardMaster(int numShards) {
    this.numShards = numShards;
  }

  public interface ShardMasterCommand extends Command {}

  @Data
  public static final class Join implements ShardMasterCommand {
    private final int groupId;
    private final Set<Address> servers;
  }

  @Data
  public static final class Leave implements ShardMasterCommand {
    private final int groupId;
  }

  @Data
  public static final class Move implements ShardMasterCommand {
    private final int groupId;
    private final int shardNum;
  }

  @Data
  public static final class Query implements ShardMasterCommand {
    private final int configNum;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  public interface ShardMasterResult extends Result {}

  @Data
  public static final class Ok implements ShardMasterResult {}

  @Data
  public static final class Error implements ShardMasterResult {}

  @Data
  public static final class ShardConfig implements ShardMasterResult {
    private final int configNum;

    // groupId -> <group members, shard numbers>
    private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
  }

  @Override
  public Result execute(Command command) {
    if (command instanceof Join) {
      Join join = (Join) command;
      return handleJoin(join.groupId(), join.servers());
    }

    if (command instanceof Leave) {
      Leave leave = (Leave) command;
      return handleLeave(leave.groupId());
    }

    if (command instanceof Move) {
      Move move = (Move) command;
      return handleMove(move.groupId(), move.shardNum());
    }

    if (command instanceof Query) {
      Query query = (Query) command;
      return handleQuery(query.configNum());
    }

    throw new IllegalArgumentException();
  }

  private Result handleJoin(int groupId, Set<Address> servers) {
    if (currentConfigNum >= 0) {
      ShardConfig currentConfig = configs.get(currentConfigNum);
      if (currentConfig.groupInfo().containsKey(groupId)) {
        return new Error();
      }
    }

    Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();

    if (currentConfigNum < 0) {
      Set<Integer> allShards = new HashSet<>();
      for (int i = 1; i <= numShards; i++) {
        allShards.add(i);
      }
      newGroupInfo.put(groupId, new ImmutablePair<>(new HashSet<>(servers), allShards));
    } else {
      ShardConfig currentConfig = configs.get(currentConfigNum);
      for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry :
          currentConfig.groupInfo().entrySet()) {
        newGroupInfo.put(
            entry.getKey(),
            new ImmutablePair<>(
                new HashSet<>(entry.getValue().getLeft()),
                new HashSet<>(entry.getValue().getRight())));
      }

      newGroupInfo.put(groupId, new ImmutablePair<>(new HashSet<>(servers), new HashSet<>()));
      rebalanceShards(newGroupInfo);
    }

    int newConfigNum = (currentConfigNum < 0) ? INITIAL_CONFIG_NUM : currentConfigNum + 1;
    ShardConfig newConfig = new ShardConfig(newConfigNum, newGroupInfo);
    configs.put(newConfigNum, newConfig);
    currentConfigNum = newConfigNum;

    return new Ok();
  }

  private Result handleLeave(int groupId) {
    if (currentConfigNum < 0) {
      return new Error();
    }

    ShardConfig currentConfig = configs.get(currentConfigNum);
    if (!currentConfig.groupInfo().containsKey(groupId)) {
      return new Error();
    }

    // new group info without the leaving group
    Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();

    for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry :
        currentConfig.groupInfo().entrySet()) {
      if (entry.getKey() != groupId) {
        newGroupInfo.put(
            entry.getKey(),
            new ImmutablePair<>(
                new HashSet<>(entry.getValue().getLeft()),
                new HashSet<>(entry.getValue().getRight())));
      }
    }

    // If remaining groups, rebalance
    if (!newGroupInfo.isEmpty()) {
      Set<Integer> leavingGroupShards = currentConfig.groupInfo().get(groupId).getRight();

      if (!leavingGroupShards.isEmpty()) {
        List<Integer> remainingGroupIds = new ArrayList<>(newGroupInfo.keySet());
        Collections.sort(remainingGroupIds);

        int groupIndex = 0;
        for (int shard : leavingGroupShards) {
          int targetGroupId = remainingGroupIds.get(groupIndex);
          newGroupInfo.get(targetGroupId).getRight().add(shard);
          groupIndex = (groupIndex + 1) % remainingGroupIds.size();
        }
      }

      rebalanceShards(newGroupInfo);
    }

    int newConfigNum = currentConfigNum + 1;
    ShardConfig newConfig = new ShardConfig(newConfigNum, newGroupInfo);
    configs.put(newConfigNum, newConfig);
    currentConfigNum = newConfigNum;

    return new Ok();
  }

  private Result handleMove(int groupId, int shardNum) {
    if (currentConfigNum < 0) {
      return new Error();
    }

    if (shardNum < 1 || shardNum > numShards) {
      return new Error();
    }

    ShardConfig currentConfig = configs.get(currentConfigNum);
    if (!currentConfig.groupInfo().containsKey(groupId)) {
      return new Error();
    }

    if (currentConfig.groupInfo().get(groupId).getRight().contains(shardNum)) {
      return new Error();
    }

    // new group with moved shard
    Map<Integer, Pair<Set<Address>, Set<Integer>>> newGroupInfo = new HashMap<>();

    for (Map.Entry<Integer, Pair<Set<Address>, Set<Integer>>> entry :
        currentConfig.groupInfo().entrySet()) {
      int gid = entry.getKey();
      Set<Address> servers = new HashSet<>(entry.getValue().getLeft());
      Set<Integer> shards = new HashSet<>(entry.getValue().getRight());

      shards.remove(shardNum);

      if (gid == groupId) {
        shards.add(shardNum);
      }

      newGroupInfo.put(gid, new ImmutablePair<>(servers, shards));
    }

    int newConfigNum = currentConfigNum + 1;
    ShardConfig newConfig = new ShardConfig(newConfigNum, newGroupInfo);
    configs.put(newConfigNum, newConfig);
    currentConfigNum = newConfigNum;

    return new Ok();
  }

  private Result handleQuery(int configNum) {
    if (currentConfigNum < 0) {
      return new Error();
    }

    if (configNum == -1 || configNum >= currentConfigNum) {
      configNum = currentConfigNum; // latest configc
    }

    return configs.get(configNum);
  }

  /**
   * Rebalances shards among all groups to achieve an even distribution. Each group should have at
   * most 1 more shard than any other group.
   */
  private void rebalanceShards(Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo) {
    int numGroups = groupInfo.size();
    if (numGroups == 0) {
      return;
    }

    int quotient = numShards / numGroups;
    int remainder = numShards % numGroups;

    List<Integer> groupIds = new ArrayList<>(groupInfo.keySet());
    Collections.sort(groupIds); // For deterministic behavior

    Map<Integer, Integer> targetCounts = new HashMap<>();
    for (int i = 0; i < groupIds.size(); i++) {
      int gid = groupIds.get(i);
      int targetCount = (i < remainder) ? quotient + 1 : quotient;
      targetCounts.put(gid, targetCount);
    }

    List<Integer> excessShards = new ArrayList<>();
    for (int gid : groupIds) {
      Set<Integer> groupShards = groupInfo.get(gid).getRight();
      int currentCount = groupShards.size();
      int targetCount = targetCounts.get(gid);

      if (currentCount > targetCount) {
        List<Integer> shardsToRemove = new ArrayList<>(groupShards);
        Collections.sort(shardsToRemove, Collections.reverseOrder());

        for (int i = 0; i < currentCount - targetCount; i++) {
          int shardToRemove = shardsToRemove.get(i);
          groupShards.remove(shardToRemove);
          excessShards.add(shardToRemove);
        }
      }
    }

    Collections.sort(excessShards);

    int shardIndex = 0;
    for (int gid : groupIds) {
      Set<Integer> groupShards = groupInfo.get(gid).getRight();
      int currentCount = groupShards.size();
      int targetCount = targetCounts.get(gid);

      if (currentCount < targetCount) {
        for (int i = 0; i < targetCount - currentCount && shardIndex < excessShards.size(); i++) {
          groupShards.add(excessShards.get(shardIndex++));
        }
      }
    }
  }
}
