package com.fibabanka.dt.kafka.hdfs;

import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.fibabanka.dt.kafka.util.Constants.*;

public class HdfsSender {

    private static Logger LOGGER = LoggerFactory.getLogger(HdfsSender.class);

    public static void sendFileToHDFS(File file, String hdfsPath) {
        LOGGER.info("HiveFileSender sendFileToHDFS method has been called");
        LOGGER.info("HDFS Path: " + hdfsPath);

        Process pr = null;
        Runtime rt = Runtime.getRuntime();
        try {
            LOGGER.info("HDFS COMMAND: " + Constants.HDFS_PUT_COMMAND + Constants.OPERATION_ROOT_DIR + file.getName() + " " +
                    hdfsPath);
            pr = rt.exec(Constants.HDFS_PUT_COMMAND + Constants.OPERATION_ROOT_DIR + file.getName() + " " +
                    hdfsPath);
            InputStream error = pr.getErrorStream();
            for (int i = 0; i < error.available(); i++) {
                LOGGER.error("" + error.read());
            }
            pr.waitFor(); //wait for its completion

            //sent file should be deleted
            if (Files.exists(Paths.get(file.getAbsolutePath()))) {
                LOGGER.info(file.getName() + " is deleted from :" + file.getAbsolutePath());
                Files.delete(Paths.get(file.getAbsolutePath()));
            }

            //crc prefixed file which was created when parquet file writing should be deleted
            if(file.getName().contains(PARQUET_FILE_EXTENTION)){
                String tmpFilePath = file.getParent() + FORWARD_SLASH + DOTCHAR + file.getName() +
                        PARQUET_TEMP_FILE_EXTENTION;
                if (Files.exists(Paths.get(tmpFilePath))) {
                    LOGGER.info(file.getName() + " is deleted from :" + tmpFilePath);
                    Files.delete(Paths.get(tmpFilePath));
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error occurred when command running as process", e);
        } catch (InterruptedException e) {
            LOGGER.error("Error occurred when command running as process", e);
        }
    }
}
