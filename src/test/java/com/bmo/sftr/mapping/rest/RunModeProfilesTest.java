package com.bmo.sftr.mapping.rest;

import static org.assertj.core.api.Assertions.assertThat;

import com.bmo.sftr.mapping.kafka.KafkaRecordListener;
import com.bmo.sftr.mapping.refdata.RefDataClient;
import java.io.IOException;
import org.apache.camel.CamelContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** The run modes chosen by profile: kafka, rest, both (and file). */
class RunModeProfilesTest {

    private static Properties profile(String name) {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application-" + name + ".yml"));
        return yaml.getObject();
    }

    private static void assertMode(String name, String kafka, String rest, String web) {
        Properties p = profile(name);
        assertThat(p.getProperty("sftr.kafka.enabled")).as(name + " kafka").isEqualTo(kafka);
        assertThat(p.getProperty("sftr.rest.enabled")).as(name + " rest").isEqualTo(rest);
        assertThat(p.getProperty("spring.main.web-application-type")).as(name + " web").isEqualTo(web);
    }

    @Test
    void kafkaModeConsumesKafkaOnlyWithoutAWebServer() {
        assertMode("kafka", "true", "false", "none");
    }

    @Test
    void restModeHasTheEndpointAndNoKafka() {
        assertMode("rest", "false", "true", "servlet");
        assertThat(profile("rest").getProperty("server.port")).isEqualTo("9090");
    }

    @Test
    void bothModeHasKafkaAndTheEndpoint() {
        assertMode("both", "true", "true", "servlet");
        assertThat(profile("both").getProperty("server.port")).isEqualTo("9090");
    }

    @Test
    void fileModeTurnsKafkaOff() {
        assertThat(profile("file").getProperty("sftr.kafka.enabled")).isEqualTo("false");
    }

    /** A real context in rest mode: the endpoint is there, the Kafka consumers are not. */
    @Nested
    @SpringBootTest
    @ActiveProfiles("rest")
    class RestModeContext {

        @Autowired ApplicationContext context;
        @MockitoBean RefDataClient refDataClient;

        @DynamicPropertySource
        static void output(DynamicPropertyRegistry registry) throws IOException {
            Path dir = Files.createTempDirectory("rest-mode");
            registry.add("sftr.output.file", () -> dir.resolve("report.csv").toString());
        }

        @Test
        void hasTheEndpointAndNoRunningKafkaConsumer() {
            assertThat(context.getBeanNamesForType(TestMessageController.class)).hasSize(1);
            CamelContext camel = context.getBean(CamelContext.class);
            assertThat(camel.getRouteController().getRouteStatus("sftr-kafka-ingest").isStarted()).isFalse();
            assertThat(context.getBeanNamesForType(KafkaRecordListener.class)).isEmpty();
        }
    }
}
