package com.fibabanka.dt.kafka.util;

import com.fibabanka.dt.kafka.hive.DaoOperationServiceImplV2;
import com.fibabanka.dt.kafka.model.avro.DecimalLogicalType;
import com.fibabanka.dt.kafka.model.avro.Field;
import com.fibabanka.dt.kafka.model.avro.RootAvroSchema;
import com.fibabanka.dt.kafka.model.avro.Type;
import com.fibabanka.dt.kafka.services.DaoOperationServiceV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.fibabanka.dt.kafka.util.Constants.DECIMAL;

public class AvroSchemaCreatorV2 {

    private static RootAvroSchema rootAvroSchema;
    private static DaoOperationServiceV2 daoOperationService;
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaCreatorV2.class);

    public static String getAvroSchema(String environment, String databaseName, String tableName) {
        LOGGER.info("Avro schema creator has been called for table " + tableName);
        rootAvroSchema = new RootAvroSchema();
        List<Field> fields = new ArrayList<>();

        daoOperationService = new DaoOperationServiceImplV2();
        Map<String, String> hiveDataTypeMapping =
                daoOperationService.getDataTypeMappingFromHive(environment, databaseName, tableName);

        rootAvroSchema.setNamespace(Constants.AVRO_NAMESPACE);
        rootAvroSchema.setName(tableName);
        rootAvroSchema.setType("record");

        for (Map.Entry<String, String> entry : hiveDataTypeMapping.entrySet()) {
            int pr;
            int sc;
            if (entry.getValue().contains(DECIMAL)) {
                String split[] = entry.getValue().substring(entry.getValue().indexOf("(") + 1, entry.getValue().indexOf(")")).split(",");
                pr = Integer.valueOf(split[0]);
                sc = Integer.valueOf(split[1]);

                fields.add(new Field(entry.getKey(), new DecimalLogicalType(entry.getKey(), "fixed", "decimal",
                        ByteLengthCalculator.getByteLength(pr), pr, sc)));
            } else {
                fields.add(new Field(entry.getKey(), new Type(entry.getValue())));
            }
        }
        rootAvroSchema.setFields(fields);
        LOGGER.info("Avro schema has been created for table " + tableName);
        LOGGER.info(rootAvroSchema.toString());
        return rootAvroSchema.toString();
    }
}