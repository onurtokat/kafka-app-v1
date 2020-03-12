package com.fibabanka.dt.kafka.hive;

import com.fibabanka.dt.kafka.services.DaoOperationServiceV2;
import com.fibabanka.dt.kafka.services.HiveOperationServiceV2;

public class HiveOperationImplV2 implements HiveOperationServiceV2 {

    private DaoOperationServiceV2 daoOperationService = new DaoOperationServiceImplV2();

    private final String databaseName;
    private final String tableName;
    private final String environment;
    private final String operationType;

    private boolean databaseStatus;
    private boolean tableStatus;

    public HiveOperationImplV2(String databaseName, String tableName, String environment, String operationType) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.environment = environment;
        this.operationType = operationType;
    }

    @Override
    public void start() {
        checkDatabase();
        if (!databaseStatus) {
            createDatabase();
        }
        checkTable();
        if (!tableStatus) {
            createTable();
        }
    }

    @Override
    public void checkDatabase() {
        databaseStatus = daoOperationService.databaseExistenceCheck(environment, databaseName);
    }

    @Override
    public void createDatabase() {
        daoOperationService.createDatabase(environment, databaseName, tableName);
    }

    @Override
    public void checkTable() {
        tableStatus = daoOperationService.tableExistenceCheck(environment, databaseName, tableName);
    }

    @Override
    public void createTable() {
        daoOperationService.createTable(environment, databaseName, tableName, operationType);
    }
}
