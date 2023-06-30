package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class);


    public static void main(String[] args) {
        log.info("start sending a message to kafka");

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.36:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 20; i++) {
            String topic = "java_demo";
            String value = "hello world! " + i;
            String key = "id-" + i;

            //create Producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            //send data
            producer.send(record, new Callback() {
                //execute everytime a record is successfully sent or an exception is thrown
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("topic:{}", metadata.topic());
                        log.info("key:{}", record.key());
                        log.info("partition:{}", metadata.partition());
                        log.info("offset:{}", metadata.offset());
                        log.info("timestamp:{}", metadata.timestamp());
                    } else {
                        exception.printStackTrace();
                    }
                }
            });
        }
        //flush - sync
        producer.flush();

        //flush and close
        producer.close();
    }
}
