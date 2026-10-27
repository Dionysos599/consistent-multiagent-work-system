package cmws.kvstore;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public class KVStore implements Application {

  private final Map<String, String> map = new HashMap<>();

  public interface KVStoreCommand extends Command {}

  public interface SingleKeyCommand extends KVStoreCommand {
    String key();
  }

  @Data
  public static final class Get implements SingleKeyCommand {
    @NonNull private final String key;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  @Data
  public static final class Put implements SingleKeyCommand {
    @NonNull private final String key, value;
  }

  @Data
  public static final class Append implements SingleKeyCommand {
    @NonNull private final String key, value;
  }

  /**
   * Store {@code value} only if {@code key} has no value yet. This is the primitive that lets
   * clients claim a name (an object, a lock, a task) exactly once.
   */
  @Data
  public static final class PutIfAbsent implements SingleKeyCommand {
    @NonNull private final String key, value;
  }

  /**
   * Replace the value of {@code key} with {@code value} only if it currently equals {@code
   * expected}. A {@code null} {@code expected} means "only if the key is absent". This is the
   * single linearizable write that moves a reference (a branch head) from one version to the next.
   */
  @Data
  public static final class CompareAndSet implements SingleKeyCommand {
    @NonNull private final String key;
    private final String expected; // null: the key must not exist
    @NonNull private final String value;
  }

  public interface KVStoreResult extends Result {}

  @Data
  public static final class GetResult implements KVStoreResult {
    @NonNull private final String value;
  }

  @Data
  public static final class KeyNotFound implements KVStoreResult {}

  @Data
  public static final class PutOk implements KVStoreResult {}

  @Data
  public static final class AppendResult implements KVStoreResult {
    @NonNull private final String value;
  }

  /**
   * Outcome of a conditional write. {@code value} is what the key holds after the operation: the
   * new value on success, the value that was in the way on failure ({@code null} if the key does
   * not exist).
   */
  @Data
  public static final class ConditionalResult implements KVStoreResult {
    private final boolean success;
    private final String value;
  }

  @Override
  public KVStoreResult execute(Command command) {
    if (command instanceof Get) {
      Get g = (Get) command;
      String value = map.get(g.key());
      if (value == null) {
        return new KeyNotFound();
      }
      return new GetResult(value);
    }

    if (command instanceof Put) {
      Put p = (Put) command;
      map.put(p.key(), p.value());
      return new PutOk();
    }

    if (command instanceof Append) {
      Append a = (Append) command;
      String currentValue = map.getOrDefault(a.key(), "");
      String newValue = currentValue + a.value();
      map.put(a.key(), newValue);
      return new AppendResult(newValue);
    }

    if (command instanceof PutIfAbsent) {
      PutIfAbsent p = (PutIfAbsent) command;
      return compareAndSet(p.key(), null, p.value());
    }

    if (command instanceof CompareAndSet) {
      CompareAndSet c = (CompareAndSet) command;
      return compareAndSet(c.key(), c.expected(), c.value());
    }

    throw new IllegalArgumentException();
  }

  private ConditionalResult compareAndSet(String key, String expected, String value) {
    String current = map.get(key);
    if (Objects.equals(current, expected)) {
      map.put(key, value);
      return new ConditionalResult(true, value);
    }
    return new ConditionalResult(false, current);
  }
}
