package com.fibabanka.dt.kafka.model.avro;

public class MyLogicalType extends Type {

    private String name;
    private String logicalType;

    public MyLogicalType(String name, String type, String logicalType) {
        super(type);
        this.name = name;
        this.logicalType = logicalType;
    }

    public String getType() {
        return super.getType();
    }

    public String getLogicalType() {
        return logicalType;
    }

    public String getName() {
        return name;
    }
}
