package com.fibabanka.dt.kafka.model.avro;

public class Field {

    private String name;
    private Type type;

    public Field(String name, Type type) {
        this.name = name;
        this.type = type;
    }

    @Override
    public String toString() {
        return "{" +
                "\"name\":\"" + name + "\"" +
                ",\"type\":" + type +
                "}";
    }
}
