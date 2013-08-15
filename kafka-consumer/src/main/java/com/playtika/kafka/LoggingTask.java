package com.playtika.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author szagriichuk
 */
public class LoggingTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingTask.class);
    private int threadNumber;
    private volatile boolean isRunnable = true;
    private ConsumerTask consumerTask;

    public LoggingTask(int threadNumber, ConsumerTask consumerTask) {
        this.threadNumber = threadNumber;
        this.consumerTask = consumerTask;
    }

    @Override
    public void run() {
        int second = 1;
        int prevCountOfMessages = 0;
        while (isRunnable()) {
            int currentMessagesCount = consumerTask.countOfMessages;
            int messagesCount = currentMessagesCount - prevCountOfMessages;
            prevCountOfMessages = currentMessagesCount;
            LOGGER.info(
                    "Read " + messagesCount + " messages per " + second + "'st second on thread " + threadNumber + ".");
            sleepForOneSecond();
            second++;
        }
    }

    private void sleepForOneSecond() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public boolean isRunnable() {
        return isRunnable;
    }

    public void setRunnable(boolean runnable) {
        isRunnable = runnable;
    }
}
