package com.fibabanka.dt.kafka.hive;

import com.fibabanka.dt.kafka.dao.OracleConnectionImpl;
import com.fibabanka.dt.kafka.dao.SingletonHiveConnection;
import com.fibabanka.dt.kafka.services.HiveConnectionService;
import com.fibabanka.dt.kafka.services.DaoOperationService;
import com.fibabanka.dt.kafka.services.OracleConnectionService;
import com.fibabanka.dt.kafka.util.Constants;
import com.fibabanka.dt.kafka.dao.HiveConnectionServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class DaoOperationServiceImpl implements DaoOperationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DaoOperationServiceImpl.class);
    private boolean databaseStatus;
    private boolean tableStatus;
    private HiveConnectionService hiveConnectionService = new HiveConnectionServiceImpl();
    private OracleConnectionService oracleConnectionService = new OracleConnectionImpl();

    @Override
    public boolean databaseExistenceCheck(String connectionString, String databaseName, String tableName) {

        String sql = "";
        ResultSet resultSet = null;
        LOGGER.info("Database: " + databaseName + " is checking...");
        sql = "SHOW DATABASES";
        try {
            LOGGER.info("Database existence checking SQL: " + sql);
            hiveConnectionService.executeQuery(connectionString, databaseName, tableName, sql);
            resultSet = hiveConnectionService.getResultSet();
            while (resultSet.next()) {
                if (resultSet.getString(1).toLowerCase().equals(databaseName.toLowerCase())) {
                    databaseStatus = true;
                    break;
                } else {
                    databaseStatus = false;
                }
            }
            LOGGER.info("Database " + databaseName + " existence status:" + databaseStatus);
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("Error occurred when Database checking!!!", e);
            System.exit(-1);
        }
        return databaseStatus;
    }

    @Override
    public void createDatabase(String connectionString, String databaseName, String tableName) {
        String sql = "";
        LOGGER.info("Database " + databaseName + " creation begin...");
        sql = "CREATE DATABASE " + databaseName + " LOCATION '/data/" + databaseName + ".db'";
        hiveConnectionService.executeUpdate(connectionString, databaseName, tableName, sql);
        sql = "SHOW DATABASES";
        boolean creationStatus = databaseExistenceCheck(connectionString, databaseName, tableName);
        LOGGER.info("Database " + databaseName + " creation status: "
                + creationStatus);
        if (!creationStatus) {
            LOGGER.error("Error occurred when Database creation!!!");
            System.exit(-1);
        }
        hiveConnectionService.close();
    }

    @Override
    public boolean tableExistenceCheck(String connectionString, String databaseName, String tableName) {
        String sql = "";
        ResultSet resultSet = null;
        sql = "SHOW TABLES IN " + databaseName;
        try {
            hiveConnectionService.executeQuery(connectionString, databaseName, tableName, sql);
            resultSet = hiveConnectionService.getResultSet();
            while (resultSet.next()) {
                if (resultSet.getString(1).toLowerCase().equals(tableName.toLowerCase())) {
                    tableStatus = true;
                    LOGGER.info("All Table columns types " + getDataTypeMappingFromHive(connectionString, databaseName, tableName));
                    break;
                } else {
                    tableStatus = false;
                }
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("Error occurred when Table checking!!!", e);
            System.exit(-1);
        }
        return tableStatus;
    }

    @Override
    public void createTable(String connectionString, String databaseName, String tableName, String operationType) {
        String sql = "";
        Map<String, String> dataTypeMap = getDataTypeMappingFromOracle(connectionString, databaseName, tableName);

        StringBuilder tmpSb = new StringBuilder();
        int counter = 1;

        for (String s : dataTypeMap.keySet()) {
            String dataTypeTmp = dataTypeMap.get(s).toLowerCase();
            if (counter < dataTypeMap.size()) {
                tmpSb.append(s + " " + dataTypeMap.get(s) + ",");
            } else {
                tmpSb.append(s + " " + dataTypeMap.get(s));
            }
            counter++;
        }

        if (operationType.equals("hdfs") || operationType.equals("hive")) {
            sql = "CREATE TABLE " + databaseName + "." + tableName + "(" + tmpSb.toString() +
                    ") ROW FORMAT DELIMITED " +
                    " FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n'" +
                    " STORED AS TEXTFILE " +
                    "LOCATION '/data/" + databaseName + ".db/" + tableName + "/'"
                    + " tblproperties ('serialization.null.format'='###')";
        }
        if (operationType.equals("parquet")) {
            sql = "CREATE TABLE " + databaseName + "." + tableName + "(" + tmpSb.toString() + ") " +
                    " STORED AS PARQUET LOCATION '/data/" + databaseName + ".db/" + tableName + "/' " +
                    "TBLPROPERTIES ('OBJCAPABILITIES'='EXTREAD,EXTWRITE')";
        }

        //System.out.println("SQL sentence: " + sql);
        LOGGER.info("SQL sentence: " + sql);
        hiveConnectionService.executeUpdate(connectionString, databaseName, tableName, sql);
        tableExistenceCheck(connectionString, databaseName, tableName);
        LOGGER.info("Table " + tableName + " creation status: " + tableStatus);
        //System.out.println("Table " + tableName + " creation status: " + tableStatus);
        hiveConnectionService.close();
    }

    @Override
    public Map<String, String> getDataTypeMappingFromHive(String connectionString, String databaseName, String tableName) {
        String sql = "";
        ResultSet resultSet = null;
        Map<String, String> dataTypeMap = new HashMap<>();
        sql = "DESCRIBE " + databaseName + "." + tableName;
        try {
            hiveConnectionService.executeQuery(connectionString, databaseName, tableName, sql);
            resultSet = hiveConnectionService.getResultSet();
            while (resultSet.next()) {
                dataTypeMap.put(resultSet.getString(1),
                        resultSet.getString(2));
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("Error occurred getting data types from " + databaseName + "." + tableName);
            System.exit(-1);
        }
        return dataTypeMap;
    }

    @Override
    public Map<String, String> getDataTypeMappingFromOracle(String connectionString, String databaseName, String tableName) {
        String sql = "";
        ResultSet resultSet = null;
        Map<String, String> dataTypeMap = new HashMap<>();

        //ordered columns should be collected to HashMap
        sql = Constants.ORACLE_DATA_MAPPING_QUERY + "'" + tableName.toUpperCase() + "' ORDER BY COLUMN_ID";
        oracleConnectionService.executeQuery(connectionString, databaseName, tableName, sql);
        resultSet = oracleConnectionService.getResultSet();
        try {
            while (resultSet.next()) {
                dataTypeMap.put(resultSet.getString(3), resultSet.getString(5));
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("Error occurred getting data types from ORACLE " + databaseName + "." + tableName);
            System.exit(-1);
        }
        return dataTypeMap;
    }

    @Override
    public List<String> getColumnNamesFromHive(String connectionString, String databaseName, String tableName) {
        String sql = "";
        ResultSet resultSet = null;
        List<String> columnList = new ArrayList<>();
        sql = "DESCRIBE " + databaseName + "." + tableName;
        try {
            hiveConnectionService.executeQuery(connectionString, databaseName, tableName, sql);
            resultSet = hiveConnectionService.getResultSet();
            while (resultSet.next()) {
                columnList.add(resultSet.getString(1));
            }
            resultSet.close();
        } catch (SQLException e) {
            LOGGER.error("Error occurred getting data types from " + databaseName + "." + tableName);
        }
        return columnList;
    }
}