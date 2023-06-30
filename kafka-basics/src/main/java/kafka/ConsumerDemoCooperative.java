package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//kafka-console.consumer.sh --bootstrap-server localhost:9092 --group first-group --reset-offsets --to-earliest --execute --topic java_demo
public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {
        log.info("start consuming a message from topic");

        String bootstrapServer = "192.168.0.36:9092";
        String groupId = "second-app";
        String topic = "java_demo";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shut down");
                //this will make consumer.poll method to throw an exception to shut down
                consumer.wakeup();

                // not shutdown the consumer thread but wait until the main thread is finished
                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to the topic
            consumer.subscribe(Arrays.asList(topic));

            while(true) {
                log.info("polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records){
                    log.info("key:" + record.key() + "|value:" + record.value());
                    log.info("partition:" + record.partition() + "|offset:" + record.offset());
                }
            }
        }catch (WakeupException e){
            log.info("Excepted Wakeup Exception");
        }catch (Exception e){
            log.error("UnExcepted Exception", e);
        }finally {
            consumer.close();
            log.info("consumer now is gracefully closed");
        }
    }
}
