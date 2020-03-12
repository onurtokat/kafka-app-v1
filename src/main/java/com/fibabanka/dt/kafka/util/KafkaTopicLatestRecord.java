package com.fibabanka.dt.kafka.util;

import com.fibabanka.dt.kafka.config.KafkaConfigCreator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaTopicLatestRecord {

    public static String getLatestRecord(String topicName, Consumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList(topicName));
        consumer.poll(Duration.ofSeconds(10));
        AtomicLong maxTimestamp = new AtomicLong();
        AtomicReference<ConsumerRecord<String, String>> latestRecord = new AtomicReference<>();
        consumer.endOffsets(consumer.assignment()).forEach((topicPartition, offset) -> {
            //System.out.println("offset: " + offset);

            // seek to the last offset of each partition
            consumer.seek(topicPartition, (offset == 0) ? offset : offset - 1);

            // poll to get the last record in each partition
            consumer.poll(Duration.ofSeconds(10)).forEach(record -> {

                // the latest record in the 'topic' is the one with the highest timestamp
                if (record.timestamp() > maxTimestamp.get()) {
                    maxTimestamp.set(record.timestamp());
                    latestRecord.set(record);
                }
            });
        });
        return latestRecord.get().value();
    }

    public static String getNotNullRecord(String topicName, String environment) {
        Consumer<String, String> consumer = new KafkaConsumer<>(KafkaConfigCreator.getConfig(Constants.DEV_ENVIRONMENT, true));
        consumer.subscribe(Collections.singletonList(topicName));
        String result = null;
        //read topic from beginning
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());
        boolean status = true;

        while (status) {
            ConsumerRecords<String, String> records = consumer.poll(0);
            for (ConsumerRecord<String, String> record : records) {
                if (!record.value().contains("null")) {
                    result = record.value();
                    status = false;
                }
            }
        }
        consumer.close();
        return result;
    }
}
