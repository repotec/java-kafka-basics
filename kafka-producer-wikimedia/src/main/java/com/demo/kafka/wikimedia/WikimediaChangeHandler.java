package com.demo.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String > producer;
    private String topic;
    private static final Logger log = LoggerFactory.getLogger(EventHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String > producer, String topic){
        this.producer = producer;
        this.topic = topic;
    }
    @Override
    public void onOpen() {
    }

    @Override
    public void onClosed() {
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent)  {
        String data = messageEvent.getData();
        ProducerRecord<String, String> record = new ProducerRecord(topic, data);
        log.info("data:{}", data);

        producer.send(record);
    }

    @Override
    public void onComment(String comment) {
    }

    @Override
    public void onError(Throwable t) {
        log.error("on error event", t);
    }
}
