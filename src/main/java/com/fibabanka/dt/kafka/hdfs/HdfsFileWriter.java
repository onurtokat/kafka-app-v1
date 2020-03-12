package com.fibabanka.dt.kafka.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HdfsFileWriter {

    private static File currentFile;
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileWriter.class);

    public static void write(File file, Map<String, String> data, List<String> schema, Map<String, String> schemaMap) {
        StringBuilder line = new StringBuilder();
        currentFile = file;
        //LOGGER.info("Current File: " + file.getAbsolutePath());
        //LOGGER.info("Schema: " + schema);
        //LOGGER.info("Data content: " + data);
        try (FileWriter writer = new FileWriter(file, true);
             BufferedWriter bw = new BufferedWriter(writer)) {
            for (String column : schema) {
                line.append(data.get(column).replace("null","###") + "\t");
            }
            /*
            for (String column : schema.keySet()) {
                //LOGGER.info("Column is to find on current data: " + column);
                //LOGGER.info("value of on the data: " + data.get(column));
                line.append(data.get(column) + "\t");
            }
            */
            //LOGGER.info("the line: " + line.toString().trim());
            bw.write(line.toString().trim() + "\n");
        } catch (IOException e) {
            LOGGER.info("Error occurred when file writing operation", e);
            System.exit(-1);
        }
    }

    public static File getCurrentFile() {
        return currentFile;
    }
}
