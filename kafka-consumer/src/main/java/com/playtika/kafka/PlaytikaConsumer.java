package com.playtika.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author szagriichuk
 */
public class PlaytikaConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlaytikaConsumer.class);
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    private KafkaProperties consumerProperties;

    public PlaytikaConsumer() {
        consumerProperties = new KafkaProperties("consumer.properties");
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Load consumer properties" + consumerProperties);
        }
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(createConsumerProperties()));
        this.topic = consumerProperties.get("topic");
    }

    public static void main(String[] args) {
        PlaytikaConsumer consumer = new PlaytikaConsumer();
        consumer.run();
        consumer.sleepFor10Seconds();
        consumer.shutdown();
    }

    private void sleepFor10Seconds() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {
            LOGGER.error(ie.getMessage(), ie);
        }
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
    }

    public void run() {
        int numThreads = Integer.parseInt(consumerProperties.get("threads.number"));
        List<KafkaStream<Message>> streams = readListOfStreams(numThreads);
        executor = Executors.newFixedThreadPool(numThreads);
        int threadNumber = 0;
        for (final KafkaStream<Message> stream : streams) {
            LOGGER.info("Start new thread" + threadNumber + "form message Stream");
            executor.submit(new ConsumerTask(stream, threadNumber));
            threadNumber++;
        }
    }

    private List<KafkaStream<Message>> readListOfStreams(int numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, numThreads);
        Map<String, List<KafkaStream<Message>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        return consumerMap.get(topic);
    }

    private Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put("zk.connect", consumerProperties.get("zkConnect"));
        props.put("groupid", consumerProperties.get("group"));
        props.put("zk.sessiontimeout.ms", "400");
        props.put("zk.synctime.ms", "200");
        props.put("autocommit.interval.ms", "1000");
        return props;
    }
}
