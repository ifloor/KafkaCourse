package br.com.monitoratec.emissor.emissor.kafka.config;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaAvroDeserializerErrorHandled extends KafkaAvroDeserializer {
    private static final Logger log = LoggerFactory.getLogger(KafkaAvroDeserializerErrorHandled.class);

    public KafkaAvroDeserializerErrorHandled(SchemaRegistryClient client) {
        super(client);
    }

    public KafkaAvroDeserializerErrorHandled(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        try {
            return super.deserialize(s, bytes);
        } catch (Exception e) {
            log.error("Error on avro deserialization", e);
            return null;
        }
    }

    @Override
    public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
        try {
            return super.deserialize(s, bytes, readerSchema);
        } catch (Exception e) {
            log.error("Error on avro deserialization", e);
            return null;
        }
    }
}

