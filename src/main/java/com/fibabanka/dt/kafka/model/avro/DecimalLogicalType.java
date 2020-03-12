package com.fibabanka.dt.kafka.model.avro;

public class DecimalLogicalType extends MyLogicalType {

    private String name;
    private int size;
    private int precision;
    private int scale;

    public DecimalLogicalType(String name, String type, String LogicalType, int size, int precision, int scale) {
        super(name, type, LogicalType);
        this.size = size;
        this.precision = precision;
        this.scale = scale;
    }

    @Override
    public String toString() {
        return "[{" +
                "\"type\":\"" + super.getType() + "\"" +
                ",\"logicalType\":\"" + super.getLogicalType() + "\"" +
                ",\"name\":\"" + super.getName() + "Type" + "\"" +
                ", \"size\":" + this.size +
                ", \"precision\":" + this.precision +
                ", \"scale\":" + this.scale +
                "},\"null\"]";
        /*
        return "{\"name\":\"" + super.getName() + "\"" +
                ",\"type\":[{" +
                "\"type\":\"" + super.getType() + "\"" +
                ",\"logicalType\":\"" + super.getLogicalType() + "\"" +
                ",\"name\":\"" + this.name + "\"" +
                ", \"size\":\"" + this.size + "\"" +
                ", \"precision\":\"" + this.precision + "\"" +
                ", \"scale\":\"" + this.scale + "\"" +
                "},\"null\"]}";
        */
    }
}
