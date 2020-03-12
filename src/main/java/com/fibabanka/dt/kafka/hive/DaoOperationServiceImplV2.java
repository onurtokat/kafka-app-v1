package com.fibabanka.dt.kafka.hive;

import com.fibabanka.dt.kafka.dao.*;
import com.fibabanka.dt.kafka.services.DaoOperationServiceV2;
import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fibabanka.dt.kafka.util.Constants.*;

public class DaoOperationServiceImplV2 implements DaoOperationServiceV2 {

    private static final Logger LOGGER = LoggerFactory.getLogger(DaoOperationServiceImplV2.class);
    private boolean databaseStatus;
    private boolean tableStatus;
    private static Map<String, String> dataTypeMapping = null;

    @Override
    public boolean databaseExistenceCheck(String environment, String databaseName) {
        LOGGER.info("Database: " + databaseName + " is being checked on Hive meta Store");
        String sql = "SHOW DATABASES";
        LOGGER.info("Database existence checking SQL: " + sql);
        try (Connection conn = SingletonHiveConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                if (resultSet.getString(1).toLowerCase().equals(databaseName.toLowerCase())) {
                    databaseStatus = true;
                    break;
                } else {
                    databaseStatus = false;
                }
            }
            resultSet.close();
            LOGGER.info("Database " + databaseName + " existence status:" + databaseStatus);
        } catch (SQLException e) {
            LOGGER.error("ERROR occurred when Database existence checking: ", e);
        }
        return databaseStatus;
    }

    @Override
    public void createDatabase(String environment, String databaseName, String tableName) {
        LOGGER.info("Database " + databaseName + " creation has been began.");
        String sql = "CREATE DATABASE " + databaseName + " LOCATION '/data/" + databaseName + ".db'";
        try (Connection conn = SingletonHiveConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE);
            statement.executeUpdate(sql);
            LOGGER.info("Database " + databaseName + " has been created successfully.");
        } catch (SQLException e) {
            LOGGER.error("ERROR occurred when Database creation!!!");
            System.exit(-1);
        }
    }

    @Override
    public boolean tableExistenceCheck(String environment, String databaseName, String tableName) {
        LOGGER.info("Table " + tableName + " is being checked on Hive Meta Store.");
        String sql = "SHOW TABLES IN " + databaseName;
        try (Connection conn = SingletonHiveConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                if (resultSet.getString(1).toLowerCase().equals(tableName.toLowerCase())) {
                    tableStatus = true;
                    LOGGER.info("All Table columns types " + getDataTypeMappingFromHive(environment, databaseName, tableName));
                    break;
                } else {
                    tableStatus = false;
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("ERROR occurred when Table checking!!!", e);
            System.exit(-1);
        }
        return tableStatus;
    }

    @Override
    public void createTable(String environment, String databaseName, String tableName, String operationType) {
        LOGGER.info("Table " + tableName + " is being created on Hive Meta Store.");
        String sql = "";
        Map<String, String> dataTypeMap = getDataTypeMappingFromOracle(environment, databaseName, tableName);
        StringBuilder tmpSb = new StringBuilder();
        int counter = 1;
        for (String s : dataTypeMap.keySet()) {
            String dataTypeTmp = dataTypeMap.get(s).toLowerCase();
            /*
            if (dataTypeTmp.contains(DECIMAL)) {
                String[] split = dataTypeTmp.substring(dataTypeTmp.indexOf(LEFTBRAKETS) + 1,
                        dataTypeTmp.indexOf(RIGHTBRAKETS)).split(COMMA);
                if (Integer.parseInt(split[1]) == 0) {
                    dataTypeTmp = "bigint";
                }
            }
            */
            if (counter < dataTypeMap.size()) {
                tmpSb.append(s + " " + dataTypeTmp + ",");
            } else {
                tmpSb.append(s + " " + dataTypeTmp);
            }
            counter++;
        }
        if (operationType.equals(HIVE_OPERATION) || operationType.equals(FILE_OPERATION)) {
            sql = "CREATE TABLE " + databaseName + "." + tableName + "(" + tmpSb.toString() +
                    ") ROW FORMAT DELIMITED " +
                    " FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'" +
                    " STORED AS TEXTFILE " +
                    "LOCATION '/data/" + databaseName + ".db/" + tableName + "/'"
                    + " tblproperties ('serialization.null.format'='###')";
        }
        if (operationType.equals(PARQUET_OPERATION) || operationType.equals(HIVE_OPERATION)
                || operationType.equals(IMPALA_OPERATION)) {
            sql = "CREATE TABLE " + databaseName + "." + tableName + "(" + tmpSb.toString() + ") " +
                    " STORED AS PARQUET LOCATION '/data/" + databaseName + ".db/" + tableName + "/' ";
        }

        LOGGER.info("SQL for Hive Table creation: " + sql);
        try (Connection conn = SingletonHiveConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE);
            statement.executeUpdate(sql);
            LOGGER.info("Table " + tableName + " has been created successfully.");
        } catch (SQLException e) {
            LOGGER.info("ERROR occurred while table " + tableName + " was creating", e);
            System.exit(-1);
        } finally {
            createStagingDirectory(tableName);
        }
    }

    @Override
    public Map<String, String> getDataTypeMappingFromHive(String environment, String databaseName, String tableName) {
        String sql = "DESCRIBE " + databaseName + DOTCHAR + tableName;
        Map<String, String> dataTypeMap = new HashMap<>();
        LOGGER.info("SQL: " + sql);
        try (Connection conn = SingletonHiveConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE + WHITESPACE + databaseName + DOTCHAR + tableName);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                dataTypeMap.put(resultSet.getString(1),
                        resultSet.getString(2));
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("ERROR occurred getting data types from " + databaseName + "." + tableName, e);
            System.exit(-1);
        }
        return dataTypeMap;
    }

    @Override
    public Map<String, String> getDataTypeMappingFromOracle(String environment, String databaseName, String tableName) {
        String sql = "";
        Map<String, String> dataTypeMap = new HashMap<>();
        //ordered columns should be collected to HashMap
        sql = Constants.ORACLE_DATA_MAPPING_QUERY + "'" + tableName.toUpperCase() + "' ORDER BY COLUMN_ID";
        try (Connection conn = SingletonOracleConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
                dataTypeMap.put(resultSet.getString(3), resultSet.getString(5));
            }
        } catch (SQLException e) {
            LOGGER.error("ERROR occurred getting data types from ORACLE table" + databaseName + "." + tableName, e);
            System.exit(-1);
        }
        return dataTypeMap;
    }

    @Override
    public List<String> getColumnNamesFromHive(String environment, String databaseName, String tableName) {
        String sql = "DESCRIBE " + databaseName + "." + tableName;
        List<String> columnList = new ArrayList<>();
        try (Connection conn = SingletonHiveConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE + WHITESPACE + databaseName + DOTCHAR + tableName);
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                columnList.add(resultSet.getString(1));
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("ERROR occurred getting data types from Hive table " + databaseName + "." + tableName, e);
            System.exit(-1);
        }
        return columnList;
    }

    @Override
    public void insert(String environment, String databaseName, String tableName, ArrayList<Map<String, String>> data) {
        if (dataTypeMapping == null) {
            dataTypeMapping = getDataTypeMappingFromHive(environment, databaseName, tableName);
        }

        /*
        StringBuilder stringMerger = new StringBuilder();
        int counter = 1;
        StringBuilder stringMerger2 = new StringBuilder();
        int counter2 = 1;
        for (String s : data.get(1).keySet()) {
            stringMerger2.append(s);
            if (counter2 < data.get(1).size()) {
                stringMerger2.append(",");
            }
            counter2++;
        }
        */
/*
        for (String s : data.get(1).keySet()) {
            if (dataTypeMapping.get(s).contains(DECIMAL)) {
                String tmp = dataTypeMapping.get(s);
                String[] split = tmp.substring(tmp.indexOf(LEFTBRAKETS) + 1,
                        tmp.indexOf(RIGHTBRAKETS)).split(COMMA);
                int scale = Integer.parseInt(split[1]);
                BigDecimal myDecimalValue = new BigDecimal(data.get(1).get(s));
                //First we need to make sure the BigDecimal matches our schema scale:
                myDecimalValue = myDecimalValue.setScale(scale, RoundingMode.HALF_UP);
                stringMerger.append(myDecimalValue);
            } else if(dataTypeMapping.get(s).contains("bigint")){
                stringMerger.append( data.get(1).get(s));
            }else {
                stringMerger.append("'" + data.get(1).get(s) + "'");
            }
            if (counter < data.get(1).size()) {
                stringMerger.append(",");
            }
            counter++;
        }
        */
        StringBuilder stringMerger = new StringBuilder();
        int counter = 1;
        for (String s : data.get(1).keySet()) {
            stringMerger.append("'" + data.get(1).get(s) + "'");
            if (counter < data.get(1).size()) {
                stringMerger.append(",");
            }
            counter++;
        }
        String sql = "INSERT INTO " + databaseName + "." + tableName + " VALUES(" + stringMerger + ")";
        //String sql = "INSERT INTO " + databaseName + "." + tableName + "("+stringMerger2+") VALUES(" + stringMerger + ")";
        //sql = "INSERT OVERWRITE TABLE " + databaseName + "." + tableName + " VALUES(" + stringMerger + ")";
        try (Connection conn = SingletonImpalaConnection.getInstance().getConnection(environment, databaseName);
             Statement statement = conn.createStatement()) {
            //statement.execute(INVALIDATE + WHITESPACE + databaseName + DOTCHAR + tableName);
            statement.executeUpdate(sql);
            LOGGER.info("INSERT SQL STATEMENT: " + sql);
        } catch (SQLException e) {
            LOGGER.info("ERROR occurred while insert into table " + tableName, e);
            System.exit(-1);
        }
    }

    @Override
    public void update(String environment, String databaseName, String tableName) {
        //TODO
    }

    @Override
    public void delete(String environment, String databaseName, String tableName) {
        //TODO
    }

    @Override
    public void truncate(String environment, String databaseName, String tableName) {
        //TODO
    }

    private void createStagingDirectory(String tableName) {
        // create operation files on table directory
        Process pr = null;
        Runtime rt = Runtime.getRuntime();

        try {
            pr = rt.exec(HDFS_CREATE_DIR_COMMAND + WHITESPACE + HDFS_ROOT_FILE_PATH +
                    tableName + KAFKA_OPERATION_DIR);
            LOGGER.info(HDFS_CREATE_DIR_COMMAND + WHITESPACE + HDFS_ROOT_FILE_PATH +
                    tableName + KAFKA_OPERATION_DIR);
            pr.waitFor(); //wait for its completion

            InputStream error = pr.getErrorStream();
            for (int i = 0; i < error.available(); i++) {
                LOGGER.error("" + error.read());
                System.exit(-1);
            }
        } catch (IOException e) {
            LOGGER.error("Error occurred when hdfs copying operation: ", e);
        } catch (InterruptedException e) {
            LOGGER.error("Error occurred when hdfs copying operation: ", e);
        }
    }
}