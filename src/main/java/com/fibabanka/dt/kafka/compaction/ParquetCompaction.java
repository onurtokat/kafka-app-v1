package com.fibabanka.dt.kafka.compaction;

import com.fibabanka.dt.kafka.services.CompactionService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.fibabanka.dt.kafka.util.Constants.*;

/**
 * @author OnurTokat
 * ParquetCompaction class merges multiple parquet files under hdfs path
 */
public class ParquetCompaction implements CompactionService {

    //Logger
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetCompaction.class);

    /**
     * merge method accept hdfs path as parameter.
     *
     * @param hdfsPath
     */
    @Override
    public void merge(String hdfsPath, String databaseName, String tableName) {

        //creating SparkSession
        SparkSession sparkSession = SparkSession.builder().appName(SPARK_APP_NAME).getOrCreate();

        //table data is loaded to spark object. For the performance improvement for the merging
        //result is cached for using coalesce operation
        //Dataset<Row> df = sparkSession.read().table(databaseName + DOTCHAR + tableName).cache();
        Dataset<Row> df = sparkSession.read().parquet(hdfsPath).cache();
        df.coalesce(COALESCE_NUM).write().parquet(hdfsPath + SPARK_OPERATION_DIR);
        sparkSession.stop();

        //hdfs copy, move, and remove operations are done using HDFS cli commands
        Process pr = null;
        Runtime rt = Runtime.getRuntime();

        try {
            pr = rt.exec(HDFS_REMOVE_FILE_COMMAND + hdfsPath + FORWARD_SLASH + DOTCHAR);
            //STARCHAR + DOTCHAR + PARQUET);
            pr.waitFor();
            pr = rt.exec(HDFS_MOVE_COMMAND + hdfsPath + SPARK_OPERATION_DIR + FORWARD_SLASH +
                    DOTCHAR + WHITESPACE + hdfsPath);
            pr.waitFor();
            pr = rt.exec(HDFS_REMOVE_DIR_COMMAND + hdfsPath + SPARK_OPERATION_DIR + FORWARD_SLASH + DOTCHAR);
            pr.waitFor();
        } catch (IOException e) {
            LOGGER.error("Error occurred when Parquet file compaction: " + e);
        } catch (InterruptedException e) {
            LOGGER.error("Error occurred when Parquet file compaction: " + e);
        }
    }
}
