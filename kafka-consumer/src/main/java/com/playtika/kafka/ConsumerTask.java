package com.playtika.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author szagriichuk
 */
public class ConsumerTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerTask.class);
    public static volatile int countOfMessages;
    private KafkaStream<Message> kafkaStream;
    private LoggingTask loggingTask;
    private int threadNumber;
    private List<String> readData = new ArrayList<String>();

    public ConsumerTask(KafkaStream<Message> stream, int threadNumber) {
        loggingTask = new LoggingTask(threadNumber);
        this.threadNumber = threadNumber;
        kafkaStream = stream;
    }

    public void run() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(loggingTask);
        ConsumerIterator<Message> consumerIterator = kafkaStream.iterator();
        while (consumerIterator.hasNext()) {
            String stringData = readStringData(consumerIterator);
            this.readData.add(stringData);
            countOfMessages++;
        }
        loggingTask.setRunnable(false);
        executorService.shutdown();
        LOGGER.info("Shutting down Thread: " + threadNumber);
    }

    private String readStringData(ConsumerIterator<Message> it) {
        MessageAndMetadata<Message> data = it.next();
        ByteBuffer payload = data.message().payload();
        return new String(payload.array());
    }

    public List<String> getReadData() {
        return readData;
    }
}
