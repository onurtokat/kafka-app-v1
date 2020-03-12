package com.fibabanka.dt.kafka.dao;

import com.fibabanka.dt.kafka.services.HiveConnectionService;
import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class HiveConnectionServiceImpl implements HiveConnectionService {

    private ResultSet resultSet = null;
    private static final Logger LOG = LoggerFactory.getLogger(HiveConnectionServiceImpl.class);
    private Connection connection = null;
    private Statement statement = null;

    @Override
    public void executeQuery(String connectionString, String databaseName, String tableName, String query) {
        LOG.info("ConnectionClass constructor has been called with database name: "
                + databaseName + ", Table name: " + tableName);
        LOG.info("Query: " + query);

        try {
            if (connection != null) {
                statement = connection.createStatement();
                setResultSet(statement.executeQuery(query));
            } else {
                statement = getConnection(connectionString, databaseName, tableName).createStatement();
                setResultSet(statement.executeQuery(query));
            }
        } catch (SQLException e) {
            LOG.error("Error occured when doing SQL execute query operation", e);
        }
    }

    @Override
    public void executeUpdate(String connectionString, String databaseName, String tableName, String query) {
        LOG.info("ConnectionClass constructor has been called with database name: "
                + databaseName + ", Table name: " + tableName);
        LOG.info("Query: " + query);

        try {
            if (connection != null) {
                statement = connection.createStatement();
                statement.executeUpdate(query);
            } else {
                statement = getConnection(connectionString, databaseName, tableName).createStatement();
                statement.executeUpdate(query);
            }
        } catch (SQLException e) {
            LOG.error("Error occured when doing SQL execute query operation", e);
        }
    }

    @Override
    public Connection getConnection(String connectionString, String databaseName, String tableName) {
        try {
            Class.forName(Constants.HIVE_DRIVER);
        } catch (ClassNotFoundException e) {
            LOG.error("Error occured when Hive Driver loading", e);
        }
        LOG.info("Connection URL: " + connectionString + databaseName + ";" + Constants.HIVE_SECURED);
        try {
            connection = DriverManager.getConnection(connectionString
                            + databaseName + ";" + Constants.HIVE_SECURED, Constants.HDFS_USER,
                    Constants.HDFS_PASSWORD);
        } catch (SQLException e) {
            LOG.error("Error occurred when getting Connection", e);
            System.exit(-1);
        }
        return connection;
    }

    private void setResultSet(ResultSet resultSet) {
        this.resultSet = resultSet;
    }

    @Override
    public ResultSet getResultSet() {
        return this.resultSet;
    }

    @Override
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.error("Error occurred when Connection closing procedure", e);
        }
    }
}
