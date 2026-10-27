package cmws.gateway;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import dslabs.framework.testing.junit.DSLabsJUnitTest;
import dslabs.framework.testing.junit.DSLabsTestRunner;
import dslabs.framework.testing.junit.Lab;
import dslabs.framework.testing.junit.Part;
import dslabs.framework.testing.junit.TestDescription;
import dslabs.framework.testing.junit.TestPointValue;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/** The HTTP gateway end to end: real sockets, a real cluster, replicas that come and go. */
@Lab("5")
@Part(6)
@RunWith(DSLabsTestRunner.class)
public class GatewayTest extends DSLabsJUnitTest {
  private GatewayServer server;
  private HttpClient http;
  private String base;

  @Before
  public void setup() throws Exception {
    StoreCluster cluster = new StoreCluster(2, 3, 10, 8);
    server = new GatewayServer(cluster, "127.0.0.1", 0);
    server.start();
    http = HttpClient.newHttpClient();
    base = "http://127.0.0.1:" + server.port();
  }

  @After
  public void teardown() {
    server.close();
  }

  private HttpResponse<String> call(String method, String path, String body) throws Exception {
    HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path));
    b.method(method, HttpRequest.BodyPublishers.ofString(body == null ? "" : body));
    return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  private static String key(String k) {
    return URLEncoder.encode(k, StandardCharsets.UTF_8).replace("+", "%20");
  }

  @Test(timeout = 60 * 1000)
  @TestPointValue(10)
  @TestDescription("Basic operations over HTTP, including awkward keys and values")
  public void test01BasicOperations() throws Exception {
    assertEquals(404, call("GET", "/kv/missing", null).statusCode());

    String awkward = "dir/file name+plus?&=%";
    assertEquals(200, call("PUT", "/kv/" + key(awkward), "line1\nline2 \"quoted\"").statusCode());
    HttpResponse<String> got = call("GET", "/kv/" + key(awkward), null);
    assertEquals(200, got.statusCode());
    assertEquals("line1\nline2 \"quoted\"", got.body());

    assertEquals("a", call("POST", "/kv/log/append", "a").body());
    assertEquals("ab", call("POST", "/kv/log/append", "b").body());

    assertEquals("{\"success\":true,\"value\":\"first\"}",
        call("POST", "/kv/name/put-if-absent", "first").body());
    assertEquals("{\"success\":false,\"value\":\"first\"}",
        call("POST", "/kv/name/put-if-absent", "second").body());
  }

  @Test(timeout = 60 * 1000)
  @TestPointValue(10)
  @TestDescription("Compare-and-set over HTTP moves a reference forward exactly once per version")
  public void test02CompareAndSet() throws Exception {
    assertEquals("{\"success\":true,\"value\":\"v1\"}", call("POST", "/kv/head/cas", "v1").body());
    assertEquals("{\"success\":false,\"value\":\"v1\"}", call("POST", "/kv/head/cas", "x").body());
    assertEquals("{\"success\":true,\"value\":\"v2\"}",
        call("POST", "/kv/head/cas?expected=v1", "v2").body());
    assertEquals("{\"success\":false,\"value\":\"v2\"}",
        call("POST", "/kv/head/cas?expected=v1", "v3").body());
    assertEquals("v2", call("GET", "/kv/head", null).body());
  }

  @Test(timeout = 120 * 1000)
  @TestPointValue(15)
  @TestDescription("The service keeps working while one replica of every group is down")
  public void test03SurvivesReplicaFailures() throws Exception {
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      keys.add("key-" + i);
      assertEquals(200, call("PUT", "/kv/key-" + i, "before-" + i).statusCode());
    }

    assertEquals(200, call("POST", "/admin/kill?node=server1-3", null).statusCode());
    assertEquals(200, call("POST", "/admin/kill?node=server2-1", null).statusCode());
    assertTrue(call("GET", "/admin/status", null).body().contains("server1-3"));

    // Reads see the old data and new writes succeed with a replica gone from each group.
    for (int i = 0; i < 20; i++) {
      assertEquals("before-" + i, call("GET", "/kv/key-" + i, null).body());
      assertEquals(200, call("PUT", "/kv/key-" + i, "during-" + i).statusCode());
    }

    // Replicas rejoin and catch up; the data is still all there.
    call("POST", "/admin/revive?node=server1-3", null);
    call("POST", "/admin/revive?node=server2-1", null);
    for (int i = 0; i < 20; i++) {
      assertEquals("during-" + i, call("GET", "/kv/key-" + i, null).body());
    }
  }

  @Test(timeout = 120 * 1000)
  @TestPointValue(15)
  @TestDescription("Concurrent HTTP clients advancing one counter with CAS lose no updates")
  public void test04ConcurrentCasOverHttp() throws Exception {
    int clients = 4, increments = 10;
    AtomicInteger wins = new AtomicInteger();
    List<Thread> threads = new ArrayList<>();
    List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
    for (int c = 0; c < clients; c++) {
      Thread t =
          new Thread(
              () -> {
                try {
                  for (int i = 0; i < increments; i++) {
                    while (true) {
                      HttpResponse<String> cur = call("GET", "/kv/counter", null);
                      String expected = cur.statusCode() == 404 ? null : cur.body();
                      int next = expected == null ? 1 : Integer.parseInt(expected) + 1;
                      String path = "/kv/counter/cas" + (expected == null ? "" : "?expected=" + expected);
                      if (call("POST", path, String.valueOf(next)).body().startsWith("{\"success\":true")) {
                        wins.incrementAndGet();
                        break;
                      }
                    }
                  }
                } catch (Throwable e) {
                  errors.add(e);
                }
              });
      threads.add(t);
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    assertTrue(errors.toString(), errors.isEmpty());
    assertEquals(clients * increments, wins.get());
    assertEquals(String.valueOf(clients * increments), call("GET", "/kv/counter", null).body());
  }
}
