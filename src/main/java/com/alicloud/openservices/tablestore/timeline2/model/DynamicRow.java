package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.util.*;

/**
 * An object that can contain fields of any type.
 *
 * @param <T>
 */
public class DynamicRow<T> {
    private Map<String, Column> fields;

    public DynamicRow() {
        fields = new HashMap<String, Column>();
    }

    public Map<String, Column> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    /**
     * Set field with value of string type, replace old value if it is exist.
     *
     * @param name field's name
     * @param value field's new value
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(String name, String value) {
        fields.put(name, new Column(name, ColumnValue.fromString(value)));
        return (T)this;
    }

    /**
     * Set field with value of boolean type, replace old value if it is exist.
     *
     * @param name field's name
     * @param value field's new value
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(String name, boolean value) {
        fields.put(name, new Column(name, ColumnValue.fromBoolean(value)));
        return (T)this;
    }

    /**
     * Set field with value of double type, replace old value if it is exist.
     *
     * @param name field's name
     * @param value field's new value
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(String name, double value) {
        fields.put(name, new Column(name, ColumnValue.fromDouble(value)));
        return (T)this;
    }

    /**
     * Set field with value of long type, replace old value if it is exist.
     *
     * @param name field's name
     * @param value field's new value
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(String name, long value) {
        fields.put(name, new Column(name, ColumnValue.fromLong(value)));
        return (T)this;
    }

    /**
     * Set field with value of string list type, replace old value if it is exist.
     *
     * @param name field's name
     * @param value field's new value
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(String name, List<String> value) {
        Gson gson = new Gson();
        fields.put(name, new Column(name, ColumnValue.fromString(gson.toJson(value))));
        return (T)this;
    }

    /**
     * Set field with value of string list type, replace old value if it is exist.
     *
     * @param columns list of field
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setFields(List<Column> columns) {
        for (Column column : columns) {
            fields.put(column.getName(), column);
        }
        return (T)this;
    }

    /**
     * Set field with value of string array type, replace old value if it is exist.
     *
     * @param name field's name
     * @param value field's new value
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(String name, String[] value) {
        Gson gson = new Gson();
        fields.put(name, new Column(name, ColumnValue.fromString(gson.toJson(value))));
        return (T)this;
    }

    /**
     * Add new column, replace old value if it is exist.
     *
     * @param column new column to add
     * @return this
     */
    @SuppressWarnings("unchecked")
    public T setField(Column column) {
        fields.put(column.getName(), column);
        return (T)this;
    }

    /**
     * Check if field is exist.
     *
     * @param name the field's name
     * @return true if exist, else false.
     */
    public boolean contains(String name) {
        return fields.containsKey(name);
    }

    /**
     * Get string value of the specified field.
     *
     * @param name the field's name
     * @return string value
     * @throws IllegalStateException if the value is not string type
     * @throws NullPointerException if the field is not exist
     */
    public String getString(String name) {
        return fields.get(name).getValue().asString();
    }

    /**
     * Get long value of the specified field.
     *
     * @param name the field's name
     * @return string value
     * @throws IllegalStateException if the value is not long type
     * @throws NullPointerException if the field is not exist
     */
    public long getLong(String name) {
        return fields.get(name).getValue().asLong();
    }

    /**
     * Get boolean value of the specified field.
     *
     * @param name the field's name
     * @return string value
     * @throws IllegalStateException if the value is not boolean type
     * @throws NullPointerException if the field is not exist
     */
    public boolean getBoolean(String name) {
        return fields.get(name).getValue().asBoolean();
    }

    /**
     * Get double value of the specified field.
     *
     * @param name the field's name
     * @return string value
     * @throws IllegalStateException if the value is not double type
     * @throws NullPointerException if the field is not exist
     */
    public double getDouble(String name) {
        return fields.get(name).getValue().asDouble();
    }

    /**
     * Get string list value of the specified field.
     *
     * @param name the field's name
     * @return string value
     * @throws IllegalStateException if the value is not string list type
     * @throws NullPointerException if the field is not exist
     */
    public List<String> getStringList(String name) {
        String jsonArray = fields.get(name).getValue().asString();
        Gson gson = new Gson();
        try {
            String[] strArray = gson.fromJson(jsonArray, String[].class);
            return Arrays.asList(strArray);
        } catch (JsonSyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        List<Map.Entry<String,Column>> list = new ArrayList<Map.Entry<String,Column>>(fields.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<String,Column>>(){

            @Override
            public int compare(Map.Entry<String, Column> o1, Map.Entry<String, Column> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        for (Map.Entry<String,Column> entry : list) {
            if (sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(entry.getValue().getName() + ":" + entry.getValue().getValue());
        }
        return sb.toString();
    }
}
