package com.bmo.sftr.mapping.refdata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bmo.sftr.mapping.TestSupport;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.client.HttpClientErrorException;
import tools.jackson.databind.JsonNode;

class RefDataClientTest {

    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();
    private final List<String> bodies = new CopyOnWriteArrayList<>();

    /** status.apply(requestNumber starting at 1) -> HTTP status; a negative value sleeps 1s, then answers 200. */
    private RefDataClient start(IntFunction<Integer> status, int maxAttempts) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/ref", ex -> {
            int n = requests.incrementAndGet();
            bodies.add(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            int s = status.apply(n);
            try {
                if (s < 0) Thread.sleep(1000);
                byte[] out = "{\"ok\":{\"values\":{\"lei\":\"L\"}}}".getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().add("Content-Type", "application/json");
                ex.sendResponseHeaders(s < 0 ? 200 : s, s < 0 || s == 200 ? out.length : -1);
                if (s < 0 || s == 200) ex.getResponseBody().write(out);
            } catch (InterruptedException | IOException ignored) {
                // client gave up
            } finally {
                ex.close();
            }
        });
        server.start();
        return new RefDataClient(new RefDataClient.Properties(
                "http://127.0.0.1:" + server.getAddress().getPort(), "/ref",
                Duration.ofSeconds(1), Duration.ofMillis(300), maxAttempts, Duration.ofMillis(10),
                Map.of("leKey", "bmoLegalEntity", "cp", "counterpartyId")));
    }

    @AfterEach
    void stop() {
        if (server != null) server.stop(0);
    }

    private static JsonNode msg(String json) {
        return TestSupport.MAPPER.readTree(json);
    }

    @Test
    void retriesOn503ThenSucceeds() throws Exception {
        RefDataClient c = start(n -> n <= 2 ? 503 : 200, 3);
        JsonNode r = c.fetch(msg("{\"bmoLegalEntity\":\"LE\"}"));
        assertThat(r.get("ok").get("values").get("lei").asText()).isEqualTo("L");
        assertThat(requests).hasValue(3);
    }

    @Test
    void doesNotRetryOn4xx() throws Exception {
        RefDataClient c = start(n -> 400, 3);
        assertThatThrownBy(() -> c.fetch(msg("{}"))).isInstanceOf(HttpClientErrorException.class);
        assertThat(requests).hasValue(1);
    }

    @Test
    void persistent503FailsAfterMaxAttempts() throws Exception {
        RefDataClient c = start(n -> 503, 3);
        assertThatThrownBy(() -> c.fetch(msg("{}")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("failed after 3 attempts");
        assertThat(requests).hasValue(3);
    }

    @Test
    void nullFieldsAreOmittedAndKeysFollowConfig() throws Exception {
        RefDataClient c = start(n -> 200, 1);
        c.fetch(msg("{\"bmoLegalEntity\":\"LE\",\"counterpartyId\":null,\"other\":\"z\"}"));
        JsonNode body = msg(bodies.get(0));
        assertThat(body.get("leKey").asText()).isEqualTo("LE");
        assertThat(body.has("cp")).isFalse();
        assertThat(body.has("other")).isFalse();
    }

    @Test
    void readTimeoutTriggersRetry() throws Exception {
        RefDataClient c = start(n -> n == 1 ? -1 : 200, 2);
        JsonNode r = c.fetch(msg("{}"));
        assertThat(r.has("ok")).isTrue();
        assertThat(requests).hasValue(2);
    }
}
