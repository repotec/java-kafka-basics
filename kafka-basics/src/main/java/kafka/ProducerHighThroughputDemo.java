package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerHighThroughputDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerHighThroughputDemo.class);


    public static void main(String[] args) {
        log.info("start sending a message to kafka");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.36:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //set high throughput producer (kafka 2.8 or lower)
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        //the number of milliseconds a producer is willing to wait before sending a batch out.
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create Producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("java_demo", "hello-world!");

        //send data
        producer.send(record);

        //flush - sync
        producer.flush();

        //flush and close
        producer.close();
    }
}
