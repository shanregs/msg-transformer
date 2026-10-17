package com.bmo.sftr.mapping.rest;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The title and description shown at the top of Swagger UI.
 *
 * <p>Swagger UI: {@code /swagger-ui.html}. The OpenAPI document: {@code /v3/api-docs}.
 */
@Configuration
public class OpenApiConfig {

    @Bean
    OpenAPI sftrOpenApi() {
        return new OpenAPI().info(new Info()
                .title("SFTR Mapping Engine")
                .version("0.1.0")
                .description("`POST /api/trades/process` reads the RECEIVED rows of the trade table, maps each to an "
                        + "SFTR row and writes the CSV. `POST /api/test-message` runs one message through the same "
                        + "flow, for testing."));
    }
}
