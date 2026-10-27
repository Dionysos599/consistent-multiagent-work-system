package cmws.gateway;

import cmws.kvstore.KVStore.Append;
import cmws.kvstore.KVStore.AppendResult;
import cmws.kvstore.KVStore.CompareAndSet;
import cmws.kvstore.KVStore.ConditionalResult;
import cmws.kvstore.KVStore.Get;
import cmws.kvstore.KVStore.GetResult;
import cmws.kvstore.KVStore.KeyNotFound;
import cmws.kvstore.KVStore.Put;
import cmws.kvstore.KVStore.PutIfAbsent;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

/**
 * HTTP front end of a {@link StoreCluster}. Keys travel percent-encoded in the path, values in the
 * request body, so any string works.
 *
 * <pre>
 *   GET  /kv/{key}                         200 value | 404
 *   PUT  /kv/{key}            body=value   200
 *   POST /kv/{key}/append     body=value   200 body=resulting value
 *   POST /kv/{key}/put-if-absent  body=value        200 {"success":..,"value":..}
 *   POST /kv/{key}/cas?expected=E body=value        200 {"success":..,"value":..}
 *        (omit "expected" to require that the key does not exist)
 *   GET  /health                           200
 *   GET  /admin/status                     200 {"groups":..,"replicas":..,"down":[..]}
 *   POST /admin/kill?node=server1-2        200      (cut a node off the network)
 *   POST /admin/revive?node=server1-2      200
 * </pre>
 *
 * A request that gets no answer from the cluster in time (no quorum) fails with 504.
 */
public final class GatewayServer implements AutoCloseable {
  private static final long TIMEOUT_MILLIS = 20_000;

  private final StoreCluster cluster;
  private final HttpServer http;
  private final ExecutorService workers = Executors.newFixedThreadPool(32);

  public GatewayServer(StoreCluster cluster, String host, int port) throws IOException {
    this.cluster = cluster;
    this.http = HttpServer.create(new InetSocketAddress(host, port), 0);
    this.http.createContext("/", this::handle);
    this.http.setExecutor(workers);
  }

  public void start() {
    http.start();
  }

  public int port() {
    return http.getAddress().getPort();
  }

  @Override
  public void close() {
    http.stop(0);
    workers.shutdownNow();
    cluster.close();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Routing
   * ---------------------------------------------------------------------------------------------*/
  private void handle(HttpExchange ex) throws IOException {
    try {
      String path = ex.getRequestURI().getRawPath();
      String method = ex.getRequestMethod();

      if (path.equals("/health")) {
        reply(ex, 200, "text/plain", "ok");
      } else if (path.equals("/admin/status")) {
        reply(ex, 200, "application/json", status());
      } else if (path.equals("/admin/kill") && method.equals("POST")) {
        cluster.kill(requiredParam(ex, "node"));
        reply(ex, 200, "application/json", status());
      } else if (path.equals("/admin/revive") && method.equals("POST")) {
        cluster.revive(requiredParam(ex, "node"));
        reply(ex, 200, "application/json", status());
      } else if (path.startsWith("/kv/")) {
        kv(ex, path.substring("/kv/".length()), method);
      } else {
        reply(ex, 404, "text/plain", "not found");
      }
    } catch (BadRequest e) {
      reply(ex, 400, "text/plain", e.getMessage());
    } catch (TimeoutException e) {
      reply(ex, 504, "text/plain", e.getMessage());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      reply(ex, 503, "text/plain", "interrupted");
    } catch (RuntimeException e) {
      reply(ex, 500, "text/plain", String.valueOf(e));
    }
  }

  private void kv(HttpExchange ex, String rest, String method)
      throws IOException, InterruptedException, TimeoutException {
    String[] parts = rest.split("/", -1);
    String key = decode(parts[0]);
    String op = parts.length > 1 ? parts[1] : "";
    if (key.isEmpty() || parts.length > 2) {
      throw new BadRequest("expected /kv/{key}[/operation]");
    }

    if (op.isEmpty() && method.equals("GET")) {
      Result r = cluster.execute(new Get(key), TIMEOUT_MILLIS);
      if (r instanceof KeyNotFound) {
        reply(ex, 404, "text/plain", "");
      } else {
        reply(ex, 200, "text/plain", ((GetResult) r).value());
      }
    } else if (op.isEmpty() && method.equals("PUT")) {
      cluster.execute(new Put(key, body(ex)), TIMEOUT_MILLIS);
      reply(ex, 200, "text/plain", "ok");
    } else if (op.equals("append") && method.equals("POST")) {
      Result r = cluster.execute(new Append(key, body(ex)), TIMEOUT_MILLIS);
      reply(ex, 200, "text/plain", ((AppendResult) r).value());
    } else if (op.equals("put-if-absent") && method.equals("POST")) {
      reply(ex, 200, "application/json", conditional(new PutIfAbsent(key, body(ex))));
    } else if (op.equals("cas") && method.equals("POST")) {
      String expected = optionalParam(ex, "expected");
      reply(ex, 200, "application/json", conditional(new CompareAndSet(key, expected, body(ex))));
    } else {
      throw new BadRequest("unsupported operation");
    }
  }

  private String conditional(Command c) throws InterruptedException, TimeoutException {
    ConditionalResult r = (ConditionalResult) cluster.execute(c, TIMEOUT_MILLIS);
    return "{\"success\":" + r.success() + ",\"value\":" + json(r.value()) + "}";
  }

  private String status() {
    List<String> down = new ArrayList<>(cluster.down());
    Collections.sort(down);
    StringBuilder sb = new StringBuilder();
    for (String d : down) {
      if (sb.length() > 0) {
        sb.append(',');
      }
      sb.append(json(d));
    }
    return "{\"groups\":"
        + cluster.groups()
        + ",\"replicas\":"
        + cluster.replicas()
        + ",\"down\":["
        + sb
        + "]}";
  }

  /* -----------------------------------------------------------------------------------------------
   *  Helpers
   * ---------------------------------------------------------------------------------------------*/
  private static final class BadRequest extends RuntimeException {
    BadRequest(String message) {
      super(message);
    }
  }

  private static String body(HttpExchange ex) throws IOException {
    return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
  }

  private static String decode(String raw) {
    // Path segments use %20 for spaces; a literal '+' must stay a '+'.
    return URLDecoder.decode(raw.replace("+", "%2B"), StandardCharsets.UTF_8);
  }

  private static String optionalParam(HttpExchange ex, String name) {
    String query = ex.getRequestURI().getRawQuery();
    if (query == null) {
      return null;
    }
    for (String pair : query.split("&")) {
      int eq = pair.indexOf('=');
      String k = eq < 0 ? pair : pair.substring(0, eq);
      if (k.equals(name)) {
        return eq < 0 ? "" : URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
      }
    }
    return null;
  }

  private static String requiredParam(HttpExchange ex, String name) {
    String v = optionalParam(ex, name);
    if (v == null) {
      throw new BadRequest("missing query parameter " + name);
    }
    return v;
  }

  private static void reply(HttpExchange ex, int status, String type, String body)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    ex.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
    ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) {
      ex.getResponseBody().write(bytes);
    }
    ex.close();
  }

  static String json(String s) {
    if (s == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder("\"");
    for (char c : s.toCharArray()) {
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.append('"').toString();
  }
}
