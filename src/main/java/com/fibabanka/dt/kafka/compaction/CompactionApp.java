package com.fibabanka.dt.kafka.compaction;

import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.fibabanka.dt.kafka.util.Constants.PARQUET;
import static com.fibabanka.dt.kafka.util.Constants.TEXT_FILE;

public class CompactionApp {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompactionApp.class);

    public static void main(String[] args) {
        String topicName;
        String operationType;
        String databaseName = null;
        String tableName = null;

        if (args.length < 3) {
            LOGGER.error("Topic name (first argument), Environment Type (second argument), Operation Type (third argument) " +
                    "should be entered");
            System.exit(-1);
        }

        topicName = args[0].toLowerCase();
        operationType = args[2];

        if (topicName.contains(".")) {
            String[] pair = topicName.split("\\.");
            databaseName = pair[0];
            tableName = pair[1];
        } else {
            LOGGER.error("Topic name must have dot character. Otherwise, database name and table name cannot be set");
            System.exit(-1);
        }

        switch (operationType.toLowerCase()) {
            case (TEXT_FILE):
                //new FileCompaction(topicName, databaseName, tableName, environmentType);
                //break;
            case (PARQUET):
                new ParquetCompaction().merge(Constants.HDFS_ROOT_FILE_PATH + tableName, databaseName, tableName);
                break;
            default:
                break;
        }
    }
}
