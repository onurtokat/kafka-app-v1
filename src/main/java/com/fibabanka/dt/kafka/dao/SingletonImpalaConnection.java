package com.fibabanka.dt.kafka.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.fibabanka.dt.kafka.util.Constants.*;
import static com.fibabanka.dt.kafka.util.Constants.DEV_ENVIRONMENT;

public class SingletonImpalaConnection {

    private static SingletonImpalaConnection instance = null;
    private Connection connection = null;
    private String connectionString = null;
    private String environment = null;
    private String databaseName = null;

    private static final Logger LOG = LoggerFactory.getLogger(SingletonImpalaConnection.class);

    private SingletonImpalaConnection() {

    }

    public Connection getConnection(String environment, String databaseName) {
        this.environment = environment;
        if (environment.equals(DEV_ENVIRONMENT)) {
            this.connectionString = DEV_IMPALA_CONNECTION_STRING;
        } else {
            this.connectionString = PROD_IMPALA_CONNECTION_STRING;
        }
        this.databaseName = databaseName;
        try {
            Class.forName(IMPALA_DRIVER);
            this.connection = DriverManager.getConnection(connectionString
                            + databaseName + SEMICOLON + IMPALA_SECURED, HDFS_USER,
                    HDFS_PASSWORD);
        } catch (ClassNotFoundException e) {
            LOG.error("Error occured when Impala Driver loading", e);
        } catch (SQLException e) {
            LOG.error("Error occured when Impala Driver loading", e);
        }
        LOG.info("Connection URL: " + connectionString + databaseName + SEMICOLON + IMPALA_SECURED);
        return this.connection;
    }

    public static SingletonImpalaConnection getInstance() {
        if (instance == null) {
            synchronized (SingletonImpalaConnection.class) {
                if (instance == null) {
                    instance = new SingletonImpalaConnection();
                }
            }
        }
        return instance;
    }
}
