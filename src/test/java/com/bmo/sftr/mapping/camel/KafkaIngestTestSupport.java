package com.bmo.sftr.mapping.camel;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Helpers for the tests that need a real broker on localhost:19092 (run with -Dkafka.it=true). */
final class KafkaIngestTestSupport {

    static final String BROKERS = "localhost:19092";

    private KafkaIngestTestSupport() {}

    static void createTopic(String topic) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERS);
        try (AdminClient ac = AdminClient.create(p)) {
            ac.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
        }
    }

    static void send(String topic, String... values) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", BROKERS);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            for (String v : values) prod.send(new ProducerRecord<>(topic, v)).get();
        }
    }

    static boolean awaitLines(Path file, int lines, long maxMs) throws Exception {
        long end = System.currentTimeMillis() + maxMs;
        while (System.currentTimeMillis() < end) {
            if (Files.exists(file) && Files.readAllLines(file).size() >= lines) return true;
            Thread.sleep(100);
        }
        return false;
    }

    static String message(int n) {
        return "{\"trade_ref\":\"" + n + "\",\"trade_type\":\"PO\",\"bmoLegalEntity\":\"LDNBB01\"}";
    }
}
