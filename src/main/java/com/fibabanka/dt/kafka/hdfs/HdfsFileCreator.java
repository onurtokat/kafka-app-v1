package com.fibabanka.dt.kafka.hdfs;

import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HdfsFileCreator {

    private static File file;
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsFileCreator.class);

    public static void create(String fileName) {
        String path = Constants.OPERATION_ROOT_DIR + fileName + "_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern(Constants.DATETIME_FORMAT));
        file = new File(path);
        try {
            file.createNewFile();
            LOGGER.info(file.getAbsolutePath() + " is empty: " + (file.length() == 0));
        } catch (IOException e) {
            LOGGER.info("Error occurred when " + file.getAbsolutePath() + " is being created");
        }
        LOGGER.info(path + " has been created");
    }

    public static File getFile() {
        return file;
    }
}
