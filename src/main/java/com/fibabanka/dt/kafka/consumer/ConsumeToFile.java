package com.fibabanka.dt.kafka.consumer;

import com.fibabanka.dt.kafka.config.KafkaConfigCreator;
import com.fibabanka.dt.kafka.hive.DaoOperationServiceImpl;
import com.fibabanka.dt.kafka.hdfs.HdfsFileCreator;
import com.fibabanka.dt.kafka.hdfs.HdfsSender;
import com.fibabanka.dt.kafka.hdfs.HdfsFileWriter;
import com.fibabanka.dt.kafka.services.DaoOperationService;
import com.fibabanka.dt.kafka.util.GoldenGateJsonParser;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author OnurTokat
 * ConsumeToFile class writes data which collected from Kafka topic into text file.
 */
public class ConsumeToFile {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeToFile.class);

    private String environment;
    private String topicName;
    private String databaseName;
    private String tableName;
    //table column name and data type mapping
    private static Map<String, String> schemaMap;
    //table column list
    private static List<String> schema;
    private static DaoOperationService daoOperationService = new DaoOperationServiceImpl();
    private boolean offsetReset;

    public ConsumeToFile(String topicName, String databaseName, String tableName, String environment, boolean offsetReset) {
        this.topicName = topicName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.environment = environment;
        this.offsetReset = offsetReset;
        start();
    }

    /**
     * start method reads data from topic and writes to file.
     * Each one minute file is closed and new file is created
     */
    private void start() {
        String hdfsFilePath = " /data/" + databaseName.toLowerCase() + ".db/" + tableName + "/";
        Consumer<String, String> consumer = new KafkaConsumer<>(KafkaConfigCreator.getConfig(environment, offsetReset));
        consumer.subscribe(Collections.singletonList(topicName));

        LocalTime startTime = LocalTime.now();
        HdfsFileCreator.create(topicName);

        //consume from beginning
        if (offsetReset) {
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(0);
            for (ConsumerRecord<String, String> record : records) {
                ArrayList<Map<String, String>> list = GoldenGateJsonParser.parseAsArrayList(record.value());

                if (schema == null || schemaMap == null) {// one time checking
                    schemaMap = daoOperationService.getDataTypeMappingFromHive(this.environment, this.databaseName, this.tableName);
                    schema = daoOperationService.getColumnNamesFromHive(this.environment, this.databaseName, this.tableName);
                }

                if (LocalTime.now().isAfter(startTime.plusSeconds(60))) {
                    LOGGER.info("60 SECONDS\t60 SECONDS\t60 SECONDS\t60 SECONDS\t60 SECONDS");
                    if (HdfsFileCreator.getFile().length() != 0) {

                        HdfsSender.sendFileToHDFS(HdfsFileCreator.getFile(), hdfsFilePath);

                        HdfsFileCreator.getFile().delete();
                        LOGGER.info(HdfsFileCreator.getFile().getAbsolutePath() + " has been deleted!!!");
                        if (!HdfsFileCreator.getFile().exists()) {
                            HdfsFileCreator.create(topicName);
                        }
                    }
                    startTime = LocalTime.now();
                }
                if (list != null) {
                    HdfsFileWriter.write(HdfsFileCreator.getFile(), list.get(1), schema, schemaMap);
                }
            }

            //out of the loop time checking
            if (LocalTime.now().isAfter(startTime.plusSeconds(60))) {
                LOGGER.info("60 SECONDS\t60 SECONDS\t60 SECONDS\t60 SECONDS\t60 SECONDS");
                if (HdfsFileCreator.getFile().length() != 0) {

                    HdfsSender.sendFileToHDFS(HdfsFileCreator.getFile(), hdfsFilePath);

                    HdfsFileCreator.getFile().delete();
                    LOGGER.info(HdfsFileCreator.getFile().getAbsolutePath() + " has been deleted!!!");
                    if (!HdfsFileCreator.getFile().exists()) {
                        HdfsFileCreator.create(topicName);
                    }
                }
                startTime = LocalTime.now();
            }
        }
    }
}
