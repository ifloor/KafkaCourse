package br.com.monitoratec.vendedor.kafka.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import kafka.avro.generated.Selling;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Configuration
public class KafkaProducer {
    private static final Logger LOGGER = LogManager.getLogger(KafkaProducer.class);

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.schema-registry-server}")
    private String schemaRegistryServer;

    private Map<String, Object> producerConfigs;

    @PostConstruct
    public void init() {
        this.producerConfigs = new HashMap<>();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfigs.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryServer);
        LOGGER.warn("Kafka producer configs: {}", producerConfigs);
    }

    @Bean
    public KafkaTemplate<String, Selling> sellingKafkaTemplate() {
        KafkaTemplate<String, Selling> kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(this.producerConfigs));
        kafkaTemplate.setDefaultTopic("br.com.monitoratec.selling");
        return kafkaTemplate;
    }
}
