package com.fibabanka.dt.kafka.dao;

import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SingletonHiveConnection {

    private static SingletonHiveConnection instance = null;
    private Connection connection = null;
    private String connectionString = null;
    private String environment = null;
    private String databaseName = null;

    private static final Logger LOG = LoggerFactory.getLogger(SingletonHiveConnection.class);

    private SingletonHiveConnection() {

    }

    public Connection getConnection(String environment, String databaseName) {
        this.environment = environment;
        if (environment.equals(Constants.DEV_ENVIRONMENT)) {
            this.connectionString = Constants.DEV_HIVE_CONNECTION_STRING;
        } else {
            this.connectionString = Constants.PROD_HIVE_CONNECTION_STRING;
        }
        this.databaseName = databaseName;
        try {
            Class.forName(Constants.HIVE_DRIVER);
            this.connection = DriverManager.getConnection(connectionString
                            + databaseName + ";" + Constants.HIVE_SECURED, Constants.HDFS_USER,
                    Constants.HDFS_PASSWORD);
        } catch (ClassNotFoundException e) {
            LOG.error("Error occured when Hive Driver loading", e);
        } catch (SQLException e) {
            LOG.error("Error occured when Hive Driver loading", e);
        }
        LOG.info("Connection URL: " + connectionString + databaseName + ";" + Constants.HIVE_SECURED);
        return this.connection;
    }

    public static SingletonHiveConnection getInstance() {
        if (instance == null) {
            synchronized (SingletonHiveConnection.class) {
                if (instance == null) {
                    instance = new SingletonHiveConnection();
                }
            }
        }
        return instance;
    }


}
