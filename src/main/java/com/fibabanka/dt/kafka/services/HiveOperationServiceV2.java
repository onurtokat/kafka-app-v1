package com.fibabanka.dt.kafka.services;

public interface HiveOperationServiceV2 {

    void checkDatabase();

    void createDatabase();

    void checkTable();

    void createTable();

    void start();
}
