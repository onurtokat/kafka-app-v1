package com.fibabanka.dt.kafka.dao;

import com.fibabanka.dt.kafka.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.fibabanka.dt.kafka.util.Constants.*;
import static com.fibabanka.dt.kafka.util.Constants.IMPALA_SECURED;

public class SingletonOracleConnection {

    private static SingletonOracleConnection instance = null;
    private Connection connection = null;
    private String connectionString = null;
    private String environment = null;
    private String databaseName = null;

    private static final Logger LOG = LoggerFactory.getLogger(SingletonOracleConnection.class);

    private SingletonOracleConnection() {

    }

    public Connection getConnection(String environment, String databaseName) {
        this.connectionString = PROD_ORACLE_CONNECTION_STRING;
        try {
            Class.forName(ORACLE_DRIVER);
            this.connection = DriverManager.getConnection(
                    connectionString, Constants.ORACLE_USER, Constants.ORACLE_PASSWORD);
        } catch (ClassNotFoundException e) {
            LOG.error("Error occured when Oracle Driver loading", e);
        } catch (SQLException e) {
            LOG.error("Error occured when Oracle Driver loading", e);
        }
        LOG.info("Connection URL: " + connectionString);
        return this.connection;
    }

    public static SingletonOracleConnection getInstance() {
        if (instance == null) {
            synchronized (SingletonOracleConnection.class) {
                if (instance == null) {
                    instance = new SingletonOracleConnection();
                }
            }
        }
        return instance;
    }
}
