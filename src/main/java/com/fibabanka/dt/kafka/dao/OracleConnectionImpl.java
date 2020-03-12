package com.fibabanka.dt.kafka.dao;

import com.fibabanka.dt.kafka.util.Constants;
import com.fibabanka.dt.kafka.services.OracleConnectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static com.fibabanka.dt.kafka.util.Constants.PROD_ORACLE_CONNECTION_STRING;

public class OracleConnectionImpl implements OracleConnectionService {

    private ResultSet resultSet = null;
    private static final Logger LOG = LoggerFactory.getLogger(OracleConnectionImpl.class);
    private static Connection connection = null;
    private static Statement statement = null;

    @Override
    public void executeQuery(String connectionString, String databaseName, String tableName, String query) {

        try {
            Class.forName(Constants.ORACLE_DRIVER);
            connection = DriverManager.getConnection(
                    PROD_ORACLE_CONNECTION_STRING, Constants.ORACLE_USER, Constants.ORACLE_PASSWORD);
            statement = connection.createStatement();
            LOG.info("Oracle SQL: " + query);
            this.resultSet = statement.executeQuery(query);

        } catch (ClassNotFoundException e) {
            LOG.error("Error occurred when Oracle Driver loading", e);
        } catch (SQLException e) {
            LOG.error("Error occurred when Oracle Driver loading", e);
        }
    }

    @Override
    public ResultSet getResultSet() {
        return resultSet;
    }
}