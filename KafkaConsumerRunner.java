package kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.Logging;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class KafkaConsumerRunner<K, V> implements Runnable, Logging {
    private final KafkaConsumer<K, V> consumer;
    private final Properties props;
    private final Consumer<V> recordProcessor;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public KafkaConsumerRunner(Properties props, Consumer<V> recordProcessor) {
        this.props = props;
        this.consumer = new KafkaConsumer<>(this.props);
        this.recordProcessor = recordProcessor;
    }

    @Override
    public void run() {
        try {
            logger.info("Starting consumer with configs: " + props);
            consumer.subscribe(Collections.singletonList("customerCountries"));
            logger.info("Subscribing to topic: ");

            while (!closed.getAndSet(false)) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(100));
                for (ConsumerRecord<K, V> record : records) {
                    logger.debug("Retrieved from Kafka: " + record.value());
                    recordProcessor.accept(record.value());
                }
                consumer.commitAsync();
            }
        } catch (Exception e) {
            logger.error("Unexpected error with Kafka consumer: ", e);
        } finally {
            try {
                consumer.commitSync();
            } finally {
                consumer.close();
                logger.info("Kafka consumer was shut down.");
            }
        }
    }

    public void shutdown() {
        logger.info("Kafka consumer shutdown requested.");
        closed.set(true);
        consumer.wakeup();
    }
}
