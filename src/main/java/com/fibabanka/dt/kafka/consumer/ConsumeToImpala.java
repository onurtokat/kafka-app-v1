package com.fibabanka.dt.kafka.consumer;

import com.fibabanka.dt.kafka.config.KafkaConfigCreator;
import com.fibabanka.dt.kafka.hive.DaoOperationServiceImplV2;
import com.fibabanka.dt.kafka.services.DaoOperationServiceV2;
import com.fibabanka.dt.kafka.util.GoldenGateJsonParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class ConsumeToImpala {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeToImpala.class);

    private String environment;
    private String topicName;
    private String databaseName;
    private String tableName;
    private static DaoOperationServiceV2 daoOperationServiceV2 = new DaoOperationServiceImplV2();
    private boolean offsetReset;

    public ConsumeToImpala(String topicName, String databaseName, String tableName, String environment, boolean offsetReset) {
        this.topicName = topicName;
        this.environment = environment;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.offsetReset = offsetReset;
        start();
    }

    private void start() {
        Consumer<String, String> consumer = new KafkaConsumer<>(KafkaConfigCreator.getConfig(environment, offsetReset));
        consumer.subscribe(Collections.singletonList(topicName));

        //consume from beginning
        if (offsetReset) {
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(0);
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("KEY: " + record.key() + " , " + "VALUE:" + record.value());
                ArrayList<Map<String, String>> list = GoldenGateJsonParser.parseAsArrayList(record.value());
                String[] splitted = list.get(0).get("table").split("\\.");
                if (list != null) {
                    daoOperationServiceV2.insert(environment, databaseName, tableName, list);
                }
            }
        }
    }
}
