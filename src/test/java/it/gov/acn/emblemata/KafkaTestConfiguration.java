package it.gov.acn.emblemata;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import it.gov.acn.emblemata.model.event.BaseEvent;
import it.gov.acn.emblemata.model.event.ConstituencyCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.util.Pair;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@TestConfiguration
@ActiveProfiles("test")
public class KafkaTestConfiguration {

    public static final String CONSUMER_GROUP_ID = "test-group";

    @Autowired
    private KafkaProperties kafkaProperties;


    @Bean
    public KafkaContainer kafkaContainer() {
        KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
                .withExposedPorts(9092,9093)
                .withReuse(true);
        // it's good practice to bind to random ports to avoid conflicts
        // but I want to use the same port every time I start the container
        // there's a test that simulates the kafka broker failure and restart
        Pair<Integer, Integer> ports = TestUtil.getRandomPorsForKafka();
        kafkaContainer.setPortBindings( List.of(ports.getFirst()+":9092", ports.getSecond()+":9093"));
        kafkaContainer.start();
        return kafkaContainer;
    }

    @Bean
    @Primary
    public ProducerFactory<String, Object> producerFactory(KafkaContainer kafkaContainer) {
        Map<String, Object> props =
                new HashMap<>(kafkaProperties.buildProducerProperties(null));
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    @Primary
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<String, Object>(producerFactory);
    }


    @Bean
    public ConsumerFactory<String, ConstituencyCreatedEvent> constituencyConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer().getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);;
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new JsonDeserializer<>(ConstituencyCreatedEvent.class));
    }
    @Bean
    public DefaultErrorHandler errorHandler() {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, exception) -> {
            System.out.println("Error occurred: " + exception.getMessage());
        });
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ConstituencyCreatedEvent> constituencyKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ConstituencyCreatedEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(constituencyConsumerFactory());
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}