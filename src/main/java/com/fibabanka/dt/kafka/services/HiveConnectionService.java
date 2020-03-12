package com.fibabanka.dt.kafka.services;

import java.sql.Connection;
import java.sql.ResultSet;

public interface HiveConnectionService {

    void executeQuery(String connectionString, String databaseName, String tableName, String query);

    void executeUpdate(String connectionString, String databaseName, String tableName, String query);

    Connection getConnection(String connectionString, String databaseName, String tableName);

    ResultSet getResultSet();

    void close();
}
