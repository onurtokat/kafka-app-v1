package com.fibabanka.dt.kafka;

import com.fibabanka.dt.kafka.consumer.ConsumeToFile;
import com.fibabanka.dt.kafka.consumer.ConsumeToHive;
import com.fibabanka.dt.kafka.consumer.ConsumeToImpala;
import com.fibabanka.dt.kafka.consumer.ConsumeToParquet;
import com.fibabanka.dt.kafka.hive.HiveOperationImplV2;
import com.fibabanka.dt.kafka.services.HiveOperationServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.fibabanka.dt.kafka.util.Constants.*;

/**
 * @author OnurTokat
 * KafkaApp class is the main start point of the Kafka Application
 * It provides three operation type:
 * 1- direct hive insert,
 * 2- delimited text file,
 * 3- parquet file)
 */
public class KafkaApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApp.class);

    public static void main(String[] args) {

        //global instance fields
        String topicName;
        String environmentType;
        String operationType;
        String databaseName = null;
        String tableName = null;
        boolean offsetReset;

        // Topic name, environment type, operation type, and offset reset are collected as argument
        if (args.length < 4) {
            LOGGER.error("Topic name (first argument), Environment Type (second argument), Operation Type (third argument) " +
                    "Offet Reset (fourth argument) should be entered");
            System.exit(-1);
        }

        topicName = args[0];
        environmentType = args[1];
        operationType = args[2];
        offsetReset = Boolean.parseBoolean(args[3]);

        if (topicName.contains(".")) {
            String[] pair = topicName.split("\\.");
            databaseName = pair[0].toLowerCase();
            tableName = pair[1].toLowerCase();
        } else {
            LOGGER.error("Topic name must have dot character. Otherwise, database name and table name cannot be set");
            System.exit(-1);
        }

        //checking hive database and table existence
        HiveOperationServiceV2 hiveOperationService = new HiveOperationImplV2(databaseName, tableName, environmentType, operationType);
        hiveOperationService.start();

        switch (operationType.toLowerCase()) {
            case (HIVE_OPERATION):
                new ConsumeToHive(topicName, environmentType, offsetReset);
                break;
            case (FILE_OPERATION):
                new ConsumeToFile(topicName, databaseName, tableName, environmentType, offsetReset);
                break;
            case (PARQUET_OPERATION):
                new ConsumeToParquet(topicName, databaseName,
                        tableName, environmentType, offsetReset);
                break;
            case (IMPALA_OPERATION):
                new ConsumeToImpala(topicName, databaseName,
                        tableName, environmentType, offsetReset);
            default:
                break;
        }
    }
}
