package com.fibabanka.dt.kafka.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface DaoOperationServiceV2 {

    boolean databaseExistenceCheck(String environment, String databaseName);

    void createDatabase(String environment, String databaseName, String tableName);

    boolean tableExistenceCheck(String environment, String databaseName, String tableName);

    void createTable(String environment, String databaseName, String tableName, String operationType);

    Map<String, String> getDataTypeMappingFromHive(String environment, String databaseName, String tableName);

    Map<String, String> getDataTypeMappingFromOracle(String environment, String databaseName, String tableName);

    List<String> getColumnNamesFromHive(String environment, String databaseName, String tableName);

    void insert(String environment, String databaseName, String tableName, ArrayList<Map<String, String>> data);

    void update(String environment, String databaseName, String tableName);

    void delete(String environment, String databaseName, String tableName);

    void truncate(String environment, String databaseName, String tableName);
}
