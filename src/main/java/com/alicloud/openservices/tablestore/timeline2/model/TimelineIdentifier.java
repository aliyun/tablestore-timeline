package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The unique identifier of timeline's entry.
 */
public class TimelineIdentifier {
    private List<PrimaryKeyColumn> fields;

    private TimelineIdentifier(List<PrimaryKeyColumn> fields) {
        Preconditions.checkArgument(fields != null && fields.size() != 0,
                "The keys of identifier is null or empty.");

        this.fields = fields;
    }

    /**
     * Return all the fields of the timeline identifier.
     *
     * @return all the fields of the timeline identifier, the list returned is unmodifiable.
     */
    public List<PrimaryKeyColumn> getFields() {
        return Collections.unmodifiableList(fields);
    }

    /**
     * Return the field at the specified position.
     *
     * @param index index of fields to return
     * @throws IndexOutOfBoundsException if the index is out of bounds
     * @return the field at the specified position.
     */
    public PrimaryKeyColumn getField(int index) {
        return fields.get(index);
    }

    public static class Builder {
        private List<PrimaryKeyColumn> fields;

        public Builder() {
            fields = new ArrayList<PrimaryKeyColumn>(3);
        }

        /**
         * Add a new field of string type.
         *
         * @param name the field's name
         * @param value the field's value
         * @return this
         */
        public Builder addField(String name, String value) {
            fields.add(new PrimaryKeyColumn(name, PrimaryKeyValue.fromString(value)));
            return this;
        }

        /**
         * Add a new field of long type.
         *
         * @param name the field's name
         * @param value the field's value
         * @return this
         */
        public Builder addField(String name, long value) {
            fields.add(new PrimaryKeyColumn(name, PrimaryKeyValue.fromLong(value)));
            return this;
        }

        /**
         * Add a new field of byte array type.
         *
         * @param name the field's name
         * @param value the field's value
         * @return this
         */
        public Builder addField(String name, byte[] value) {
            fields.add(new PrimaryKeyColumn(name, PrimaryKeyValue.fromBinary(value)));
            return this;
        }

        /**
         * Add a new field.
         *
         * @param column the field
         * @return
         */
        public Builder addField(PrimaryKeyColumn column) {
            fields.add(column);
            return this;
        }

        /**
         * Create a new TimelineIdentifier.
         *
         * @return the new object
         */
        public TimelineIdentifier build() {
            TimelineIdentifier id = new TimelineIdentifier(fields);
            fields = null;
            return id;
        }
    }

    @Override
    public String toString() {
        return fields.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TimelineIdentifier)) {
            return false;
        }

        TimelineIdentifier id = (TimelineIdentifier) o;
        if (this.fields.size() != id.fields.size()) {
            return false;
        }

        for (int i = 0; i < fields.size(); i++) {
            if (!fields.get(i).equals(id.fields.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (fields == null) {
            return 0;
        }

        int result = 1;

        for (PrimaryKeyColumn field : fields) {
            result = 31 * result + (field == null ? 0 : field.hashCode());
        }

        return result;
    }
}
