package com.bmo.sftr.mapping.refdata;

import com.bmo.sftr.mapping.model.MappingFunction;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

@Component
public class RefDataClient {

    @ConfigurationProperties(prefix = "sftr.refdata")
    public record Properties(
            String baseUrl,
            String path,
            @DefaultValue("2s") Duration connectTimeout,
            @DefaultValue("5s") Duration readTimeout,
            @DefaultValue("3") int maxAttempts,
            @DefaultValue("200ms") Duration backoff,
            @DefaultValue Map<String, String> requestFields) {}

    private final Properties p;
    private final RestClient http;

    public RefDataClient(Properties p) {
        if (p.maxAttempts() < 1) throw new IllegalArgumentException("sftr.refdata.max-attempts must be >= 1");
        if (p.baseUrl() == null || p.baseUrl().isBlank()) throw new IllegalArgumentException("sftr.refdata.base-url is required");
        if (p.path() == null || p.path().isBlank()) throw new IllegalArgumentException("sftr.refdata.path is required");
        this.p = p;
        HttpClient jdk = HttpClient.newBuilder().connectTimeout(p.connectTimeout()).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(jdk);
        factory.setReadTimeout(p.readTimeout());
        this.http = RestClient.builder().baseUrl(p.baseUrl()).requestFactory(factory).build();
    }

    public JsonNode fetch(JsonNode message) {
        ObjectNode body = JsonNodeFactory.instance.objectNode();
        p.requestFields().forEach((requestKey, messageField) -> {
            String v = MappingFunction.text(message, messageField);
            if (v != null) body.put(requestKey, v);              // null values are omitted
        });

        RuntimeException last = null;
        for (int attempt = 1; attempt <= p.maxAttempts(); attempt++) {
            try {
                JsonNode r = http.post().uri(p.path())
                        .contentType(MediaType.APPLICATION_JSON).body(body)
                        .retrieve().body(JsonNode.class);
                return r != null ? r : JsonNodeFactory.instance.objectNode();
            } catch (HttpClientErrorException e) {
                throw e;                                          // 4xx: never retried
            } catch (HttpServerErrorException | ResourceAccessException e) {
                last = e;                                         // 5xx and I/O timeouts: retried
                if (attempt < p.maxAttempts()) pause(p.backoff().toMillis() * attempt);
            }
        }
        throw new UnavailableException("Reference data call failed after " + p.maxAttempts() + " attempts", last);
    }

    /** The service stayed unavailable (timeouts or 5xx) for every attempt. Transient: the record is not bad. */
    public static class UnavailableException extends IllegalStateException {
        public UnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
