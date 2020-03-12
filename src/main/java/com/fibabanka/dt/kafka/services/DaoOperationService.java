package com.fibabanka.dt.kafka.services;


import java.util.List;
import java.util.Map;

public interface DaoOperationService {

    boolean databaseExistenceCheck(String connectionString, String databaseName, String tableName);

    void createDatabase(String connectionString, String databaseName, String tableName);

    boolean tableExistenceCheck(String connectionString, String databaseName, String tableName);

    void createTable(String connectionString, String databaseName, String tableName, String operationType);

    Map<String, String> getDataTypeMappingFromHive(String connectionString, String databaseName, String tableName);

    Map<String, String> getDataTypeMappingFromOracle(String connectionString, String databaseName, String tableName);

    List<String> getColumnNamesFromHive(String connectionString, String databaseName, String tableName);
}