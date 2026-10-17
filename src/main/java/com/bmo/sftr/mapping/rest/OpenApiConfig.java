package com.bmo.sftr.mapping.rest;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The title and description shown at the top of Swagger UI. Present only when {@code sftr.rest.enabled=true}
 * (the {@code rest} or {@code both} profile), like the endpoints it describes.
 *
 * <p>Swagger UI: {@code /swagger-ui.html}. The OpenAPI document: {@code /v3/api-docs}.
 */
@Configuration
@ConditionalOnProperty(name = "sftr.rest.enabled", havingValue = "true")
public class OpenApiConfig {

    @Bean
    OpenAPI sftrOpenApi() {
        return new OpenAPI().info(new Info()
                .title("SFTR Mapping Engine: test endpoints")
                .version("0.1.0")
                .description("Quick-test endpoints, for a development machine only. "
                        + "`/api/test-message` runs one message through the whole flow (reference data, mapping, CSV). "
                        + "`/api/kafka-message` puts one message on a Kafka topic so the consumer handles it."));
    }
}
