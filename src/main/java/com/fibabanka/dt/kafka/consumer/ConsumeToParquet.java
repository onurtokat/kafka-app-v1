package com.fibabanka.dt.kafka.consumer;

import com.fibabanka.dt.kafka.config.KafkaConfigCreator;
import com.fibabanka.dt.kafka.hdfs.HdfsSenderV2;
import com.fibabanka.dt.kafka.hive.DaoOperationServiceImplV2;
import com.fibabanka.dt.kafka.services.DaoOperationServiceV2;
import com.fibabanka.dt.kafka.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import static com.fibabanka.dt.kafka.util.Constants.*;

public class ConsumeToParquet {

    private String topicName;
    private String environment;
    private String databaseName;
    private String tableName;
    private Schema schema;
    private boolean offsetReset;

    private Map<String, String> hiveDataTypeMapping = null;
    private static GenericRecord genericRecord = null;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeToParquet.class);

    public ConsumeToParquet(String topicName, String databaseName, String tableName, String environment, boolean offsetReset) {
        this.topicName = topicName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.environment = environment;
        this.offsetReset = offsetReset;
        this.schema = setAvroSchema();
        start();
    }

    private Schema setAvroSchema() {
        DaoOperationServiceV2 daoOperationService = new DaoOperationServiceImplV2();
        if (this.hiveDataTypeMapping == null) {
            this.hiveDataTypeMapping = daoOperationService.getDataTypeMappingFromHive(this.environment,
                    this.databaseName, this.tableName);
        }
        String schemaTmp = AvroSchemaCreatorV2.getAvroSchema(this.environment, this.databaseName, this.tableName);
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema avroSchema = parser.parse(schemaTmp);
        genericRecord = new GenericData.Record(avroSchema);
        return avroSchema;
    }

    private void start() {
        ArrayList<Map<String, String>> list;
        Path parquetFilePath = new Path(Constants.OPERATION_ROOT_DIR + topicName + "_" +
                LocalDateTime.now().format(DateTimeFormatter.ofPattern(Constants.DATETIME_FORMAT)) +
                Constants.PARQUET_FILE_EXTENTION);

        Consumer<String, String> consumer = new KafkaConsumer<>(KafkaConfigCreator.getConfig(environment, offsetReset));
        consumer.subscribe(Collections.singletonList(topicName));

        LocalTime startTime = LocalTime.now();

        //read topic from beginning
        if (offsetReset) {
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());
        }

        try {
            org.apache.parquet.hadoop.ParquetWriter writer = AvroParquetWriter.builder(parquetFilePath)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize(4 * 1024 * 1024) //For compression
                    .withRowGroupSize(16 * 1024 * 1024) //For write buffering (Page size)
                    .build();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(0);
                for (ConsumerRecord<String, String> record : records) {
                    list = GoldenGateJsonParser.parseAsArrayList(record.value());
                    for (Map.Entry<String, String> entry : list.get(1).entrySet()) {
                        if (!entry.getValue().contains(NULL)) {
                            if (hiveDataTypeMapping.get(entry.getKey()).contains(DECIMAL)) {
                                String dataType = hiveDataTypeMapping.get(entry.getKey());
                                String[] split = dataType.substring(dataType.indexOf(LEFTBRAKETS) + 1,
                                        dataType.indexOf(RIGHTBRAKETS)).split(COMMA);
                                int precision = Integer.parseInt(split[0]);
                                int scale = Integer.parseInt(split[1]);
                                genericRecord.put(entry.getKey().toLowerCase(), getFixed((entry.getValue().contains(NULL))
                                        ? "0" : entry.getValue(), entry.getKey().toLowerCase() + "Type", precision, scale));
                            } else {
                                genericRecord.put(entry.getKey().toLowerCase(), entry.getValue().trim());
                            }
                        }
                    }
                    writer.write(genericRecord);
                }
                if (LocalTime.now().isAfter(startTime.plusSeconds(600))) {
                    writer.close();
/*
                    HdfsSender.sendFileToHDFS(new File(parquetFilePath.toUri().toString()),
                            Constants.HDFS_ROOT_FILE_PATH + tableName.toLowerCase() + FORWARD_SLASH);
                    */

                    HdfsSenderV2.sendFileToHDFS(new File(parquetFilePath.toUri().toString()),
                            HDFS_ROOT_FILE_PATH + tableName.toLowerCase(),
                            databaseName, tableName);

                    //create new parquet file!!!
                    parquetFilePath = new Path(Constants.OPERATION_ROOT_DIR + topicName + "_" +
                            LocalDateTime.now().format(DateTimeFormatter.ofPattern(Constants.DATETIME_FORMAT)) +
                            Constants.PARQUET_FILE_EXTENTION);
                    writer = AvroParquetWriter.builder(parquetFilePath)
                            .withSchema(schema)
                            .withCompressionCodec(CompressionCodecName.SNAPPY)
                            .withPageSize(4 * 1024 * 1024) //For compression
                            .withRowGroupSize(16 * 1024 * 1024) //For write buffering (Page size)
                            .build();
                    startTime = LocalTime.now();
                    LOGGER.info("Each one minute file is sent to hdfs and new file is created to append");
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Error occured when " + parquetFilePath + " written", ex);
            System.exit(-1);
        }
    }

    private static GenericData.Fixed getFixed(String value, String name, int precision, int scale) {
        BigDecimal myDecimalValue = new BigDecimal(value);
        //First we need to make sure the BigDecimal matches our schema scale:
        myDecimalValue = myDecimalValue.setScale(scale, RoundingMode.HALF_UP);

        //Next we get the decimal value as one BigInteger (like there was no decimal point)
        BigInteger myUnscaledDecimalValue = myDecimalValue.unscaledValue();

        //Finally we serialize the integer
        byte[] decimalBytes = myUnscaledDecimalValue.toByteArray();

        //We need to create an Avro 'Fixed' type and pass the decimal schema once more here:
        GenericData.Fixed fixed = new GenericData.Fixed(new Schema.Parser().parse("{\"type\": \"fixed\", \"size\":" +
                ByteLengthCalculator.getByteLength(precision) +
                ", " +
                "\"precision\": " + precision + ", \"scale\": " + scale + ", \"name\":\"" + name + "\"}"));

        byte[] myDecimalBuffer = new byte[ByteLengthCalculator.getByteLength(precision)];
        if (myDecimalBuffer.length >= decimalBytes.length) {
            //Because we set our fixed byte array size as 16 bytes, we need to
            //pad-left our original value's bytes with zeros
            int myDecimalBufferIndex = myDecimalBuffer.length - 1;
            for (int i = decimalBytes.length - 1; i >= 0; i--) {
                myDecimalBuffer[myDecimalBufferIndex] = decimalBytes[i];
                myDecimalBufferIndex--;
            }
            //Save result
            fixed.bytes(myDecimalBuffer);
        } else {
            throw new IllegalArgumentException(String.format("Decimal size: %d was greater than the allowed max: %d",
                    decimalBytes.length, myDecimalBuffer.length));
        }
        return fixed;
    }
}