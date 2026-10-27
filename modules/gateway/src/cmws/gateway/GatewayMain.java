package cmws.gateway;

/**
 * Starts an in-process cluster and serves it over HTTP.
 *
 * <pre>
 *   java -cp ... cmws.gateway.GatewayMain [--port 8080] [--groups 2] [--replicas 3] [--shards 10]
 *       [--data-dir DIR]
 *
 * With --data-dir every replica keeps a write-ahead log there and a restart recovers the data.
 * </pre>
 */
public final class GatewayMain {
  public static void main(String[] args) throws Exception {
    int port = 8080, groups = 2, replicas = 3, shards = 10, clients = 16;
    java.nio.file.Path dataDir = null;
    for (int i = 0; i + 1 < args.length; i += 2) {
      if (args[i].equals("--data-dir")) {
        dataDir = java.nio.file.Path.of(args[i + 1]);
        continue;
      }
      int value = Integer.parseInt(args[i + 1]);
      switch (args[i]) {
        case "--port" -> port = value;
        case "--groups" -> groups = value;
        case "--replicas" -> replicas = value;
        case "--shards" -> shards = value;
        case "--clients" -> clients = value;
        default -> throw new IllegalArgumentException("unknown option " + args[i]);
      }
    }

    StoreCluster cluster = new StoreCluster(groups, replicas, shards, clients, dataDir);
    GatewayServer server = new GatewayServer(cluster, "127.0.0.1", port);
    Runtime.getRuntime().addShutdownHook(new Thread(server::close));
    server.start();
    System.out.println(
        "gateway listening on http://127.0.0.1:"
            + server.port()
            + " ("
            + groups
            + " groups x "
            + replicas
            + " replicas, "
            + shards
            + " shards"
            + (dataDir == null ? ", volatile" : ", data in " + dataDir)
            + ")");
  }
}
