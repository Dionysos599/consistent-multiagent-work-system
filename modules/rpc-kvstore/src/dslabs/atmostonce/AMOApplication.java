package dslabs.atmostonce;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application> implements Application {
  @Getter @NonNull private final T application;

  private final Map<Address, Map<Integer, Result>> results = new HashMap<>();
  private final Map<Address, Integer> highestSequenceNums = new HashMap<>();

  @Override
  public AMOResult execute(Command command) {
    if (!(command instanceof AMOCommand)) {
      throw new IllegalArgumentException();
    }

    AMOCommand amoCommand = (AMOCommand) command;
    Address clientAddress = amoCommand.clientAddress();
    int sequenceNum = amoCommand.sequenceNum();

    if (alreadyExecuted(amoCommand)) {
      // Cache hit
      Map<Integer, Result> clientResults = results.get(clientAddress);
      return new AMOResult(sequenceNum, clientResults.get(sequenceNum));
    }

    Integer highestSeqNum = highestSequenceNums.get(clientAddress);

    // Outdated command that we haven't seen before
    if (highestSeqNum != null && sequenceNum < highestSeqNum && !amoCommand.readOnly()) {
      Result dummyResult;
      if (amoCommand.command() instanceof KVStore.Put) {
        dummyResult = new KVStore.PutOk();
      } else if (amoCommand.command() instanceof KVStore.Append) {
        dummyResult = new KVStore.AppendResult("dummy");
      } else {
        dummyResult = application.execute(amoCommand.command());
      }

      if (!results.containsKey(clientAddress)) {
        results.put(clientAddress, new HashMap<>());
      }
      results.get(clientAddress).put(sequenceNum, dummyResult);

      return new AMOResult(sequenceNum, dummyResult);
    }

    // Track the highest sequenceNum seen from this client
    if (highestSeqNum == null || sequenceNum > highestSeqNum) {
      highestSequenceNums.put(clientAddress, sequenceNum);
    }

    Result result = application.execute(amoCommand.command());
    if (!results.containsKey(clientAddress)) {
      results.put(clientAddress, new HashMap<>());
    }
    results.get(clientAddress).put(sequenceNum, result);

    // Garbage collection
    if (highestSeqNum != null && highestSeqNum > 2) {
      Map<Integer, Result> clientResults = results.get(clientAddress);
      // remove all results older than highestSeqNum - 2
      clientResults.keySet().removeIf(seqNum -> seqNum < highestSeqNum - 2);
    }

    return new AMOResult(sequenceNum, result);
  }

  public Result executeReadOnly(Command command) {
    if (!command.readOnly()) {
      throw new IllegalArgumentException();
    }

    if (command instanceof AMOCommand) {
      return execute(command);
    }

    return application.execute(command);
  }

  public boolean alreadyExecuted(AMOCommand amoCommand) {
    Address clientAddress = amoCommand.clientAddress();
    int sequenceNum = amoCommand.sequenceNum();

    Map<Integer, Result> clientResults = results.get(clientAddress);
    return clientResults != null && clientResults.containsKey(sequenceNum);
  }
}
