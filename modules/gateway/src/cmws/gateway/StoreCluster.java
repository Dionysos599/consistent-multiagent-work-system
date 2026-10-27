package cmws.gateway;

import cmws.core.DurableStore;
import cmws.core.FileDurableStore;
import cmws.core.StoreProvider;
import cmws.kvstore.KVStore;
import cmws.paxos.PaxosClient;
import cmws.paxos.PaxosServer;
import cmws.shardkv.ShardStoreClient;
import cmws.shardkv.ShardStoreServer;
import cmws.shardmaster.ShardMaster;
import cmws.shardmaster.ShardMaster.Join;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.framework.testing.LocalAddress;
import dslabs.framework.testing.StateGenerator;
import dslabs.framework.testing.runner.RunSettings;
import dslabs.framework.testing.runner.RunState;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A complete sharded, replicated key/value cluster (shard masters, replica groups, Paxos inside
 * every group) running inside this process on the test framework's runtime, plus a pool of clients
 * that callers borrow to execute commands.
 *
 * <p>Nodes can be taken down and brought back ({@link #kill} / {@link #revive}) to demonstrate that
 * the service keeps working while a minority of every group is gone.
 */
public final class StoreCluster implements AutoCloseable {
  private static final int MASTERS = 3;
  private static final String CONTROLLER_PREFIX = "gateway-controller-";

  private final int groups;
  private final int replicas;
  private final RunState runState;
  private final RunSettings settings = new RunSettings();
  private final BlockingQueue<Client> pool = new LinkedBlockingQueue<>();
  private final Set<String> down = ConcurrentHashMap.newKeySet();

  private final List<FileDurableStore> stores = new ArrayList<>();

  /** A volatile cluster: everything is lost when the process ends. */
  public StoreCluster(int groups, int replicas, int numShards, int clients)
      throws InterruptedException {
    this(groups, replicas, numShards, clients, null);
  }

  /**
   * @param dataDir if not null, every replica keeps a write-ahead log under this directory, and a
   *     cluster started on the same directory recovers what the previous one had
   */
  public StoreCluster(int groups, int replicas, int numShards, int clients, Path dataDir)
      throws InterruptedException {
    this.groups = groups;
    this.replicas = replicas;

    // Client addresses carry this cluster's start time. Replicated state remembers every client it
    // has ever served (for at-most-once execution), so a restarted cluster must not reuse them.
    final long incarnation = System.currentTimeMillis();
    final Address controllerAddress = new LocalAddress(CONTROLLER_PREFIX + incarnation);

    final StoreProvider provider =
        dataDir == null
            ? null
            : node -> {
                FileDurableStore store =
                    new FileDurableStore(
                        dataDir.resolve(node.toString().replace('/', '_')).resolve("wal"));
                synchronized (stores) {
                  stores.add(store);
                }
                return store;
              };

    final Address[] masters =
        IntStream.rangeClosed(1, MASTERS).mapToObj(StoreCluster::master).toArray(Address[]::new);
    final Pattern serverName = Pattern.compile("server(\\d+)-(\\d+)");

    StateGenerator generator =
        StateGenerator.builder()
            .serverSupplier(
                a -> {
                  if (Arrays.asList(masters).contains(a)) {
                    return new PaxosServer(
                        a, masters.clone(), new ShardMaster(numShards), open(provider, a));
                  }
                  Matcher m = serverName.matcher(a.toString());
                  if (m.find()) {
                    int groupId = Integer.parseInt(m.group(1));
                    Address[] group =
                        IntStream.rangeClosed(1, replicas)
                            .mapToObj(i -> server(groupId, i))
                            .toArray(Address[]::new);
                    return new ShardStoreServer(
                        a, masters.clone(), numShards, group, groupId, provider);
                  }
                  throw new IllegalArgumentException("unknown server " + a);
                })
            .clientSupplier(
                a ->
                    a.equals(controllerAddress)
                        ? new PaxosClient(a, masters.clone())
                        : new ShardStoreClient(a, masters.clone(), numShards))
            .workloadSupplier(dslabs.framework.testing.Workload.emptyWorkload())
            .build();

    runState = new RunState(generator);
    for (Address m : masters) {
      runState.addServer(m);
    }
    for (int g = 1; g <= groups; g++) {
      for (int i = 1; i <= replicas; i++) {
        runState.addServer(server(g, i));
      }
    }
    runState.start(settings);

    // Make the groups responsible for shards.
    Client controller = runState.addClient(controllerAddress);
    for (int g = 1; g <= groups; g++) {
      final int group = g;
      Set<Address> members =
          IntStream.rangeClosed(1, replicas)
              .mapToObj(i -> server(group, i))
              .collect(Collectors.toSet());
      controller.sendCommand(new Join(g, members));
      controller.getResult();
    }

    for (int i = 0; i < clients; i++) {
      pool.add(runState.addClient(new LocalAddress("gw-" + incarnation + "-" + i)));
    }
  }

  /** Execute {@code command} and wait up to {@code timeoutMillis} for its result. */
  public Result execute(Command command, long timeoutMillis)
      throws InterruptedException, TimeoutException {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    Client client = pool.poll(timeoutMillis, TimeUnit.MILLISECONDS);
    if (client == null) {
      throw new TimeoutException("no free client");
    }

    client.sendCommand(command);
    while (!client.hasResult()) {
      if (System.nanoTime() > deadline) {
        // The client keeps retrying and cannot take another command until it has an answer. Hand it
        // back to the pool only once it does, without blocking the caller any longer.
        Thread drain =
            new Thread(
                () -> {
                  try {
                    client.getResult();
                    pool.add(client);
                  } catch (InterruptedException ignored) {
                  }
                },
                "gateway-drain");
        drain.setDaemon(true);
        drain.start();
        throw new TimeoutException("no answer within " + timeoutMillis + " ms");
      }
      Thread.sleep(2);
    }
    Result result = client.getResult();
    pool.add(client);
    return result;
  }

  /** Cut a node off the network (it neither sends nor receives). Name: server1-2, shardmaster3. */
  public void kill(String node) {
    Address a = new LocalAddress(node);
    settings.receiverActive(a, false);
    settings.senderActive(a, false);
    down.add(node);
  }

  public void revive(String node) {
    Address a = new LocalAddress(node);
    settings.receiverActive(a, true);
    settings.senderActive(a, true);
    down.remove(node);
  }

  public Set<String> down() {
    return Set.copyOf(down);
  }

  public int groups() {
    return groups;
  }

  public int replicas() {
    return replicas;
  }

  @Override
  public void close() {
    try {
      runState.stop();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    synchronized (stores) {
      for (FileDurableStore store : stores) {
        try {
          store.close();
        } catch (IOException ignored) {
        }
      }
    }
  }

  private static DurableStore open(StoreProvider provider, Address node) {
    try {
      return provider == null ? null : provider.open(node);
    } catch (IOException e) {
      throw new IllegalStateException("cannot open stable storage for " + node, e);
    }
  }

  static Address master(int i) {
    return new LocalAddress("shardmaster" + i);
  }

  static Address server(int group, int i) {
    return new LocalAddress("server" + group + "-" + i);
  }

  // Referenced so that KVStore is part of the dependency closure of this module.
  @SuppressWarnings("unused")
  private static final Class<?> APPLICATION = KVStore.class;
}
