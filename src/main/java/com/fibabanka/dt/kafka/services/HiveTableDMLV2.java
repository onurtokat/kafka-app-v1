package com.fibabanka.dt.kafka.services;

import java.util.ArrayList;
import java.util.Map;

public interface HiveTableDMLV2 {

    void operate(String connectionString, String databaseName, String tableName, ArrayList<Map<String, String>> data);
}
