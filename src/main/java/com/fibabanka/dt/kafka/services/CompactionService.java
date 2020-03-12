package com.fibabanka.dt.kafka.services;

public interface CompactionService {

    void merge(String hdfsPath, String databaseName, String table);
}
