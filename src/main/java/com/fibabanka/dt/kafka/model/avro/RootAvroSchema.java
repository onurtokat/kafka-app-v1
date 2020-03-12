package com.fibabanka.dt.kafka.model.avro;

import java.util.List;

public class RootAvroSchema {

    private String name;
    private String namespace;
    private String type;
    private List<Field> fields;

    @Override
    public String toString() {
        return "{" +
                "\"name\":\"" + name + "\"" +
                ",\"namespace\":\"" + namespace + "\"" +
                ",\"type\":\"" + type + "\"" +
                ", \"fields\":" + fields +
                "}";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}
