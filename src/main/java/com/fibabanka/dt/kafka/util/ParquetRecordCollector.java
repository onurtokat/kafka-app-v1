package com.fibabanka.dt.kafka.util;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ParquetRecordCollector {

    private GenericRecord genericRecord;
    private final Schema parquetSchema;
    private List<GenericRecord> genericRecordList = new ArrayList<>();

    public ParquetRecordCollector(Schema schema) {
        parquetSchema = schema;
        genericRecord = new GenericData.Record(schema);
    }

    public void recordMerger(Map<String, String> record) {
        for (Map.Entry<String, String> entry : record.entrySet()) {
            System.out.println("ENTRY KEY: " + entry.getKey() + ", ENTRY VALUE: " + entry.getValue());
            System.out.println(entry.getKey() + " data type: " + getParquetSchema().getField(entry.getKey().toUpperCase()).schema().getType());

            switch (parquetSchema.getField(entry.getKey().toUpperCase()).schema().getType()) {
                case INT:
                    genericRecord.put(entry.getKey().toUpperCase(), Integer.parseInt(entry.getValue().trim()));
                    break;
                case LONG:
                    genericRecord.put(entry.getKey().toUpperCase(), Long.parseLong(entry.getValue().trim()));
                    break;
                case FLOAT:
                    genericRecord.put(entry.getKey().toUpperCase(), Float.parseFloat(entry.getValue().trim()));
                    break;
                case NULL:
                    genericRecord.put(entry.getKey().toUpperCase(), null);
                    break;
                case DOUBLE:
                    genericRecord.put(entry.getKey().toUpperCase(), Double.parseDouble(entry.getValue().trim()));
                    break;
                default:
                    genericRecord.put(entry.getKey().toUpperCase(), entry.getValue());
            }
            genericRecordList.add(genericRecord);
        }
    }

    public List<GenericRecord> getGenericRecordList() {
        return genericRecordList;
    }

    public Schema getParquetSchema() {
        return parquetSchema;
    }
}
