import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.impl.SimpleLoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithGraceShutDown {
    public static final Logger log = new SimpleLoggerFactory().getLogger(ConsumerDemoWithGraceShutDown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a consumer with grace shutdown.");

        String bootstrapServer = "127.0.0.1:9092";
        String topic = "demo_java";
        String groupId = "my-second-application";

        // Config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Shutdown graceful
        final Thread mainTreed = Thread.currentThread();

        // Adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detect the shutdown, lets exit by calling consumer wakeup.");
                consumer.wakeup();

                try {
                    mainTreed.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

            // Poll for new data
            while (true) {
                log.info("Polling");

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    log.info("Key : " + consumerRecord.key() + " Value : " + consumerRecord.value());
                    log.info("Partition : " + consumerRecord.partition() + " Offset : " + consumerRecord.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("Wakeup Exception.");
        } catch (Exception e) {
            log.info(e.getMessage());
        } finally {
            consumer.close();
            log.info("Consumer close gracefully.");
        }
    }
}


