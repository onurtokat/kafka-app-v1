package com.fibabanka.dt.kafka.services;

public interface HiveOperationService {

    void setConnectionString();

    void checkDatabase();

    void createDatabase();

    void checkTable();

    void createTable();

    void start();
}