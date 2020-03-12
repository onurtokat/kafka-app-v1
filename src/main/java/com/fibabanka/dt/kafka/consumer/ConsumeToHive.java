package com.fibabanka.dt.kafka.consumer;

import com.fibabanka.dt.kafka.config.KafkaConfigCreator;
import com.fibabanka.dt.kafka.hive.HiveTableDMLImpl;
import com.fibabanka.dt.kafka.services.HiveTableDML;
import com.fibabanka.dt.kafka.util.Constants;
import com.fibabanka.dt.kafka.util.GoldenGateJsonParser;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.fibabanka.dt.kafka.util.Constants.DEV_ENVIRONMENT;

/**
 * @author OnurTokat
 * ConsumeToHive class insert record which achieved from kafka topic into hive table.
 */
public class ConsumeToHive {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeToHive.class);

    private String environment;
    private String topicName;
    private static String connectionString;
    private static HiveTableDML hiveTableDML = new HiveTableDMLImpl();
    private boolean offsetReset;

    /**
     * ConsumeToHive constructor
     *
     * @param topicName
     * @param environment
     * @param offsetReset
     */
    public ConsumeToHive(String topicName, String environment, boolean offsetReset) {
        this.topicName = topicName;
        this.environment = environment;
        this.offsetReset = offsetReset;
        start();
    }

    private void start() {
        Consumer<String, String> consumer = new KafkaConsumer<>(KafkaConfigCreator.getConfig(environment, offsetReset));
        consumer.subscribe(Collections.singletonList(topicName));

        if (environment.toLowerCase().equals(DEV_ENVIRONMENT)) {
            connectionString = Constants.DEV_HIVE_CONNECTION_STRING;
        } else {
            connectionString = Constants.PROD_HIVE_CONNECTION_STRING;
        }

        //from beginning
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
                    hiveTableDML.operate(connectionString, splitted[0], splitted[1], list);
                }
            }
        }
    }
}
