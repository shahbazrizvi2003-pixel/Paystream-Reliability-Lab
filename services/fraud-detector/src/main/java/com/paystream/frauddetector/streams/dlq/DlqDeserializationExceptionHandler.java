package com.paystream.frauddetector.streams.dlq;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Streams deserialization handler that routes poison records to the DLQ
 * (FR-07) instead of the Stage 1 log-and-skip behaviour. The raw value is
 * published with the Document 03 header set (see {@link DlqPublisher});
 * afterwards the thread CONTINUEs so the consumer keeps running. If the DLQ
 * publish itself fails, the response is FAIL so Streams retries - dropping a
 * record we could not even dead-letter would be silent data loss.
 *
 * <p>Configuration (through the Streams config map):
 * <ul>
 *   <li>bootstrap.servers - source of the DLQ producer config (always present)</li>
 *   <li>dlq.topic - target topic, default {@code bank.transactions.v1.dlq}</li>
 *   <li>dlq.consumer.group - header value, default {@code fraud-detector}</li>
 * </ul>
 */
public class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(DlqDeserializationExceptionHandler.class);

    private final Producer<byte[], byte[]> producerOverride; // test seam
    private DlqPublisher publisher;

    public DlqDeserializationExceptionHandler() {
        this(null);
    }

    DlqDeserializationExceptionHandler(Producer<byte[], byte[]> producerOverride) {
        this.producerOverride = producerOverride;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> producerConfigs = new HashMap<>();
        configs.forEach((k, v) -> producerConfigs.put(k, String.valueOf(v)));
        // bootstrap.servers may be stored as a List by the Streams runtime;
        // String.valueOf(List) produces "[host:port, ...]" with brackets which
        // KafkaProducer rejects. Re-extract it from the raw config map.
        Object rawBootstrap = configs.get("bootstrap.servers");
        if (rawBootstrap instanceof java.util.List) {
            producerConfigs.put("bootstrap.servers",
                    ((java.util.List<?>) rawBootstrap).stream()
                            .map(Object::toString)
                            .collect(java.util.stream.Collectors.joining(",")));
        }
        producerConfigs.put("key.serializer", ByteArraySerializer.class.getName());
        producerConfigs.put("value.serializer", ByteArraySerializer.class.getName());
        producerConfigs.put("acks", "all");
        producerConfigs.put("enable.idempotence", "true");

        Object dlqTopic = configs.get("dlq.topic");
        Object group = configs.get("dlq.consumer.group");
        Producer<byte[], byte[]> producer = producerOverride != null
                ? producerOverride
                : new KafkaProducer<>(producerConfigs);
        publisher = new DlqPublisher(producer,
                dlqTopic != null ? String.valueOf(dlqTopic) : "bank.transactions.v1.dlq",
                group != null ? String.valueOf(group) : "fraud-detector");
    }

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {
        log.warn("Deserialization failed on {}:{}@{} - routing to DLQ",
                context.topic(), context.partition(), context.offset());
        return publisher.publish(
                        context.topic(), context.partition(), context.offset(),
                        record.key(), record.value(),
                        exception.getClass().getName(), exception.getMessage(), 1)
                == DlqPublisher.PublishResult.PUBLISHED
                ? DeserializationHandlerResponse.CONTINUE
                : DeserializationHandlerResponse.FAIL;
    }
}
