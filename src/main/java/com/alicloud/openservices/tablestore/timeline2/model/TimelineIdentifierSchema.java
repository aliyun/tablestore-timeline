package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Schema of timeline identifier, contains each field's name and type.
 */
public class TimelineIdentifierSchema {
    private List<PrimaryKeySchema> keys;
    private TimelineIdentifierSchema(List<PrimaryKeySchema> keys) {
        this.keys = keys;
    }

    public static class Builder {
        List<PrimaryKeySchema> keys;

        public Builder() {
            keys = new ArrayList<PrimaryKeySchema>(4);
        }

        /**
         * Add a field with type of string.
         *
         * @param name field's name
         *
         * @return this
         */
        public Builder addStringField(String name) {
            keys.add(new PrimaryKeySchema(name, PrimaryKeyType.STRING));
            return this;
        }

        /**
         * Add a field with type of long.
         *
         * @param name field's name
         *
         * @return this
         */
        public Builder addLongField(String name) {
            keys.add(new PrimaryKeySchema(name, PrimaryKeyType.INTEGER));
            return this;
        }

        /**
         * Add a field with type of byte array
         *
         * @param name field's name
         *
         * @return this
         */
        public Builder addBinaryField(String name) {
            keys.add(new PrimaryKeySchema(name, PrimaryKeyType.BINARY));
            return this;
        }

        /**
         * Create a new timeline's schema.
         *
         * @return the schema object
         */
        public TimelineIdentifierSchema build() {
            TimelineIdentifierSchema schema = new TimelineIdentifierSchema(keys);
            keys = null;
            return schema;
        }
    }

    /**
     * Get the schema of timeline identifier, contains each field with name and type.
     *
     * @return all the fields
     */
    public List<PrimaryKeySchema> getKeys() {
        return Collections.unmodifiableList(keys);
    }
}
