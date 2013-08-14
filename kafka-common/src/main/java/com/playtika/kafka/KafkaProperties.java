package com.playtika.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author szagriichuk
 */
public class KafkaProperties {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProperties.class);
    private Properties kafkaProperties;

    public KafkaProperties(String name) {
        kafkaProperties = loadProperties(name);
    }

    public String get(String key) {
        return kafkaProperties.getProperty(key);
    }

    private Properties loadProperties(String name) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(name));
        } catch (IOException e) {
            LOGGER.info(e.getMessage(), e);
        }
        return properties;
    }


    @Override
    public String toString() {
        return "KafkaProperties{" +
                "kafkaProperties=" + kafkaProperties +
                '}';
    }
}
