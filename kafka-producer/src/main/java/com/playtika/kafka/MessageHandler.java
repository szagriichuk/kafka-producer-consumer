package com.playtika.kafka;

/**
 * @author szagriichuk
 */
public interface MessageHandler {
    void sendMessage(String data);
}
