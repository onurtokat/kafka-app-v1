package com.fibabanka.dt.kafka.hive;

import com.fibabanka.dt.kafka.services.HiveTableDML;
import com.fibabanka.dt.kafka.dao.HiveConnectionServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Map;

public class HiveTableDMLImpl implements HiveTableDML {
    private static String sql = null;
    private String operationType = null;
    private StringBuilder stringMerger;
    private com.fibabanka.dt.kafka.services.HiveConnectionService HiveConnectionService = new HiveConnectionServiceImpl();
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveTableDMLImpl.class);
    private StringBuilder sqlStringBuilder = null;

    @Override
    public void operate(String connectionString, String databaseName, String tableName, ArrayList<Map<String, String>> data) {
        stringMerger = new StringBuilder();
        operationType = data.get(0).get("op_type");
        int counter = 1;
        // INSERT operation
        if (operationType.equals("I")) {
            for (String s : data.get(1).keySet()) {
                stringMerger.append("'" + data.get(1).get(s) + "'");
                if (counter < data.get(1).size()) {
                    stringMerger.append(",");
                }
                counter++;
            }
            sql = "INSERT INTO " + databaseName + "." + tableName + " VALUES(" + stringMerger + ")";
            //sql = "INSERT OVERWRITE TABLE " + databaseName + "." + tableName + " VALUES(" + stringMerger + ")";
            LOGGER.info("SQL STATEMENT: " + sql);

            HiveConnectionService.executeQuery(connectionString, databaseName, tableName, sql);
        } else if (operationType.equals("U")) {
            //TODO
        } else if (operationType.equals("D")) {
            //TODO
        } else if (operationType.equals("T")) {
            //TODO
        } else {
            //TODO
        }
    }
}