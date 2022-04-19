package org.syracus.kstream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockito.MockitoAnnotations.openMocks;

@EmbeddedKafka(topics = KstreamAppApplicationTest.INPUT_TOPIC)
@DirtiesContext
@Slf4j
@ExtendWith(MockitoExtension.class)
class KstreamAppApplicationTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";
    private static final String GROUP_NAME = "inventory-count-test";
    private static final boolean AUTOCOMMIT = true;

    private static ProducerFactory<String, String> producerFactory;
    private static KafkaTemplate<String, String> kafkaTemplate;
    private static ConsumerFactory<String, String> consumerFactory;

    private static ConfigurableApplicationContext applicationContext;

    @Mock
    private SomeService someService;

    @BeforeAll
    static void init(EmbeddedKafkaBroker embeddedKafkaBroker) {
        producerFactory = createProducerFactory(embeddedKafkaBroker);
        kafkaTemplate = createKafkaTemplate(producerFactory, INPUT_TOPIC);
        consumerFactory = createConsumerFactory(embeddedKafkaBroker, AUTOCOMMIT);
        applicationContext = createApplicationContext(embeddedKafkaBroker);
    }

    @AfterAll
    static void shutdown() {
        applicationContext.close();
    }


    @BeforeEach
    void setup() {
//        final var consumer = consumerFactory.createConsumer(GROUP_NAME);
//        consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
        openMocks(this);
    }

    @AfterEach
    void teardown() {
        // reset and collect all
    }


    @Test
    void testSomething() {
        doSend("hello", "world");
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> verify(someService).doSomethingImportantWithData("world"));
    }



    private void doSend(String key, String value) {
        log.debug("sending event key {}, value {} to topic {}", key, value, INPUT_TOPIC);
        kafkaTemplate.send(INPUT_TOPIC, key, value);
        kafkaTemplate.flush();
    }

    private static ConfigurableApplicationContext createApplicationContext(EmbeddedKafkaBroker broker) {
        final var applicationContext = new SpringApplicationBuilder(KstreamAppApplication.class)
                .properties(
                        "spring.cloud.stream.kafka.streams.binder.brokers=" + broker.getBrokersAsString(),
                        "spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000",
                        "spring.cloud.stream.kafka.streams.binder.configuration.cache.max.bytes.buffering=0"
                )
                .run();
        return applicationContext;
    }

    private static KafkaTemplate<String, String> createKafkaTemplate(ProducerFactory<String, String> producerFactory, String destination) {
        final var kafkaTemplate = new KafkaTemplate<String, String>(producerFactory);
        kafkaTemplate.setDefaultTopic(destination);
        return kafkaTemplate;
    }

    private static ProducerFactory<String, String> createProducerFactory(EmbeddedKafkaBroker broker) {
        Map<String, Object> producerProperties = KafkaTestUtils.producerProps(broker);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // TODO: add additional properties
        final var producerFactory = new DefaultKafkaProducerFactory<String, String>(producerProperties);
        return producerFactory;
    }

    private static ConsumerFactory<String, String> createConsumerFactory(EmbeddedKafkaBroker broker, boolean autoCommit) {
        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(GROUP_NAME, String.valueOf(autoCommit), broker);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // TODO: add additional properties
        final var consumerFactory = new DefaultKafkaConsumerFactory<String, String>(consumerProperties);
        return consumerFactory;
    }
}