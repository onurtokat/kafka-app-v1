package com.fibabanka.dt.kafka.services;

import java.sql.ResultSet;

public interface OracleConnectionService {

    void executeQuery(String connectionString, String databaseName, String tableName, String query);

    ResultSet getResultSet();
}