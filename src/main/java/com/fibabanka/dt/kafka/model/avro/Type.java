package com.fibabanka.dt.kafka.model.avro;

public class Type {

    private String type;

    public Type(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "[\"" + getType() + "\",\"null\"]";
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}