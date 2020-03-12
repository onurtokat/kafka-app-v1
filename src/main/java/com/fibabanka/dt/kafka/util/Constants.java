package com.fibabanka.dt.kafka.util;

public class Constants {

    ///////////////////// HIVE
    // dev
    public static final String DEV_HIVE_CONNECTION_STRING = "jdbc:hive2://fbdevbigm01.fibabanka.local:10000/";
    public static final String DEV_BOOTSTRAP_SERVERS = "fbdevbigd01.fibabanka.local:9092," +
            "fbdevbigd02.fibabanka.local:9092," +
            "fbdevbigd03.fibabanka.local:9092";

    // production
    public static final String PROD_HIVE_CONNECTION_STRING = "jdbc:hive2://fbprdbige01.fibabanka.local:10000/";
    public static final String PROD_BOOTSTRAP_SERVERS = "fbprdbigd01.fibabanka.local:9092," +
            "fbprdbigd02.fibabanka.local:9092," +
            "fbprdbigd03.fibabanka.local:9092," +
            "fbprdbigd04.fibabanka.local:9092," +
            "fbprdbigd05.fibabanka.local:9092," +
            "fbprdbigd06.fibabanka.local:9092";
    public static final String PROD_ORACLE_CONNECTION_STRING = "jdbc:oracle:thin:@exa02-scan:1521/fbdwhprd";
    public static final String HIVE_SECURED = "ssl=true;AuthMech=3;SSLTrustStore=/usr/java/jdk1.8.0_181-amd64/jre/lib/security/jssecacerts";

    ///////////////////// IMPALA
    // Prod
    public static final String PROD_IMPALA_CONNECTION_STRING = "jdbc:impala://fbprdbige01.fibabanka.local:21050/";
    public static final String IMPALA_SECURED = "ssl=1;AuthMech=3;SSLTrustStore=/usr/java/jdk1.8.0_181-amd64/jre/lib/security/jssecacerts;";

    //Dev
    public static final String DEV_IMPALA_CONNECTION_STRING = "jdbc:impala://fbdevbigm01.fibabanka.local:21050/";

    //USER-PASS
    public static final String HDFS_USER = "fu_kafkauser";
    public static final String HDFS_PASSWORD = "hMzNk7R3?-";
    public static final String ORACLE_USER = "KAFKA";
    public static final String ORACLE_PASSWORD = "Fiba.2020";

    //Query
    public static final String ORACLE_DATA_MAPPING_QUERY = "SELECT owner" +
            ",table_name" +
            ",column_name" +
            ",data_type AS oracle_data_type,\n" +
            "CASE WHEN data_type = 'NUMBER' AND DATA_PRECISION IS NOT NULL AND DATA_SCALE IS NOT NULL THEN " +
            "'DECIMAL(' || DATA_PRECISION  || ',' || DATA_SCALE || ')'\n" +
            "WHEN data_type = 'NUMBER' AND (DATA_PRECISION IS NULL OR DATA_SCALE IS NULL) THEN 'DECIMAL(38,0)'\n" +
            "WHEN data_type = 'DATE' THEN 'STRING'\n" +
            "WHEN data_type LIKE 'TIMESTAMP%' THEN 'STRING'\n" +
            "ELSE 'STRING'\n" +
            "END AS hive_data_type\n" +
            ",NULLABLE " +
            "FROM ALL_TAB_COLS WHERE OWNER='ODS' and table_name=";
    public static final String INVALIDATE = "INVALIDATE METADATA";

    //Drivers
    public static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    public static final String IMPALA_DRIVER = "com.cloudera.impala.jdbc.Driver";
    public static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

    //HDFS operations
    public static final String HDFS_ROOT_FILE_PATH = "/data/ods.db/";
    public static final String HDFS_PUT_COMMAND = "hdfs dfs -put ";
    public static final String HDFS_REMOVE_FILE_COMMAND = "hdfs dfs -rm ";
    public static final String HDFS_REMOVE_DIR_COMMAND = "hdfs dfs -rm -r ";
    public static final String HDFS_MOVE_COMMAND = "hdfs dfs -mv ";
    public static final String HDFS_CREATE_DIR_COMMAND = "hdfs dfs -mkdir ";
    public static final String OPERATION_DIR = "/tmp";
    public static final String KAFKA_OPERATION_DIR = "/_kafka_staging";
    public static final String SPARK_OPERATION_DIR = "/_spark_staging";

    //SPARK Operations
    public static final String SPARK_SUBMIT = "spark-submit --master yarn --deploy-mode cluster " +
            "--class com.fibabanka.dt.spark.SparkInsertApp " +
            "hdfs://nameservice1/utils/jar_files/spark-app-v1-1.0-SNAPSHOT-jar-with-dependencies.jar ";
    public static final String SPARK_APP_NAME = "kafka-merge-app";
    public static final int COALESCE_NUM = 4;

    //File extentions
    public static final String PARQUET = "parquet";
    public static final String TEXT_FILE = "csv";

    //Data Types
    public static final String DECIMAL = "decimal";
    public static final String NULL = "null";
    public static final String TIMESTAMP = "timestamp";

    // characters
    public static final String WHITESPACE = " ";
    public static final String FORWARD_SLASH = "/";
    public static final String STARCHAR = "*";
    public static final String DOTCHAR = ".";
    public static final char RIGHTBRAKETS = ')';
    public static final char LEFTBRAKETS = '(';
    public static final String COMMA = ",";
    public static final String SEMICOLON = ";";

    //Operation Types
    public static final String HIVE_OPERATION = "hive";
    public static final String FILE_OPERATION = "textfile";
    public static final String PARQUET_OPERATION = "parquet";
    public static final String IMPALA_OPERATION = "impala";

    //Environment Type
    public static final String DEV_ENVIRONMENT = "dev";
    public static final String PROD_ENVIRONMENT = "prod";

    public static final String OPERATION_ROOT_DIR = "/tmp/kafka-app-dir/";

    public static final String DATETIME_FORMAT = "yyyyMMdd_HHmmss";

    public static final String PARQUET_FILE_EXTENTION = ".parquet";
    public static final String CSV_FILE_EXTENTION = ".csv";
    public static final String PARQUET_TEMP_FILE_EXTENTION = ".crc";

    public static final String AVRO_NAMESPACE = "com.fibabanka.dt";
    public static final int AVRO_FIXED_ARRAY_SIZE = 16;
}
