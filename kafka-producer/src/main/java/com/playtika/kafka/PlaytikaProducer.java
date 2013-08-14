package com.playtika.kafka;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Properties;

/**
 * @author szagriichuk
 */
public class PlaytikaProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PlaytikaProducer.class);
    private KafkaProperties producerProperties;
    private MessageHandler messageHandler;

    public PlaytikaProducer() {
        producerProperties = new KafkaProperties("producer.properties");
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Load producer properties" + producerProperties);
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Start producer");
        PlaytikaProducer playtikaProducer = new PlaytikaProducer();
        ProducerConfig producerConfig = playtikaProducer.createProducerConfig();
        Producer producer = playtikaProducer.createProducer(producerConfig);
        LOGGER.info("Start to send messages.");
        playtikaProducer.sendMessagesToKafka(producer);
    }

    private void sendMessagesToKafka(final Producer producer) {
        messageHandler = new MessageHandler() {
            @Override
            public void sendMessage(String data) {
                LOGGER.info("Message Handler received message " + data);
                producer.send(createProducerData(data));
            }
        };
        readData();
    }

    private ProducerData createProducerData(final String data) {
        String topic = producerProperties.get("topic");
        return new ProducerData(topic, new ArrayList<String>() {{
            add(data);
        }});
    }

    private void readData() {
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = createBufferedReader();
            readData(bufferedReader);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        } finally {
            closeBufferedReader(bufferedReader);
        }
    }

    private void readData(BufferedReader bufferedReader) throws IOException {
        String data;
        while ((data = bufferedReader.readLine()) != null) {
            messageHandler.sendMessage(data);
        }
    }

    private BufferedReader createBufferedReader() throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream("messages.plt");
        return new BufferedReader(new InputStreamReader(fileInputStream, Charset.forName("UTF-8")));
    }

    private void closeBufferedReader(BufferedReader bufferedReader) {
        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException e) {
                // nothing
            }
        }
    }

    private Producer createProducer(ProducerConfig producerConfig) {
        return new Producer(producerConfig);
    }

    private ProducerConfig createProducerConfig() {
        Properties properties = new Properties();
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("zk.connect", producerProperties.get("zkConnect"));
        return new ProducerConfig(properties);
    }
}
