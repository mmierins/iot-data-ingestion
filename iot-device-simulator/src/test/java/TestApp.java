import app.config.SimulatorAppConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import simulate.TemperatureDeviceSimulator;
import stats.KafkaStats;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = TestApp.InnerConfiguration.class)
public class TestApp {

    private static final String KAFKA_TOPIC = "test-topic-name";

    @ClassRule
    public static EmbeddedKafkaRule embeddedKafka = new EmbeddedKafkaRule(1, true, KAFKA_TOPIC);

    @Autowired
    private TemperatureDeviceSimulator simulator;
    @Autowired
    private KafkaStats kafkaStats;

    List<String> messages = new ArrayList<>();

    @Before
    public void cleanup() {
        this.messages.clear();
    }

    @Test
    public void is_loopback_working() throws InterruptedException {
        simulator.simulate();

        Thread.sleep(3000);

        assertEquals(1, kafkaStats.getSuccess());
        assertEquals(0, kafkaStats.getFailure());

        String template = "{\"data\":{\"deviceId\":\"%s\",\"location\":{\"latitude\":%d,\"longitude\":%d},\"temperature\":%d,\"time\":%d}}";
        String expected = String.format(template,
                simulator.getDataEnvelope().getData().getDeviceId(),
                simulator.getDataEnvelope().getData().getLocation().getLatitude(),
                simulator.getDataEnvelope().getData().getLocation().getLongitude(),
                simulator.getDataEnvelope().getData().getTemperature(),
                simulator.getDataEnvelope().getData().getTime());
        assertEquals(expected, messages.get(0));
    }

    @KafkaListener(topics = KAFKA_TOPIC, groupId = "group_id")
    public void consume(String message) {
        messages.add(message);
    }

    @Configuration
    static class InnerConfiguration extends SimulatorAppConfig {

        @Bean
        public KafkaStats kafkaStats() {
            return new KafkaStats();
        }

        @Bean
        public TemperatureDeviceSimulator temperatureDeviceSimulator() {
            return new TemperatureDeviceSimulator();
        }

        @Bean
        public static PropertySourcesPlaceholderConfigurer propertyConfig() {
            PropertySourcesPlaceholderConfigurer pspc = new PropertySourcesPlaceholderConfigurer();
            Properties prop = new Properties();
            prop.setProperty("kafka.topic", KAFKA_TOPIC);
            prop.setProperty("kafka.brokers", embeddedKafka.getEmbeddedKafka().getBrokersAsString());
            pspc.setProperties(prop);
            return pspc;
        }

        @Bean
        public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> kafkaListenerContainerFactory() {
            ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory());
            factory.setConcurrency(3);
            factory.getContainerProperties().setPollTimeout(3000);
            return factory;
        }

        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            return new DefaultKafkaConsumerFactory<>(consumerConfigs());
        }

        @Bean
        public Map<String, Object> consumerConfigs() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getEmbeddedKafka().getBrokersAsString());
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "group_id");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return props;
        }

    }

}
