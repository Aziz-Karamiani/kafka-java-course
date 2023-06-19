import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.impl.SimpleLoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {
    public static final Logger log = new SimpleLoggerFactory().getLogger(ProducerDemoWithKey.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a producer.");

        // Config
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "demo_java";
            String message = "Hello From Java." + i;
            String key = "id_" + i;
            // create kafka record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);

            // Send Data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Call Metadata : \n" +
                                "Topic is : " + metadata.topic() + "\n" +
                                "Key is : " + producerRecord.key() + "\n" +
                                "Partition is : " + metadata.partition() + "\n" +
                                "Offset is : " + metadata.offset() + "\n" +
                                "Timestamp is : " + metadata.timestamp()
                        );
                    } else {
                        log.error(exception.getMessage());
                    }
                }
            });
        }

        // Flush
        producer.flush();

        // Close
        producer.close();
    }
}
