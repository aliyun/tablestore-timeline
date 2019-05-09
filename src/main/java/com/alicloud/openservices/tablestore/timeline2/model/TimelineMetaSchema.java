package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.search.IndexSchema;

public class TimelineMetaSchema {
    private TimelineIdentifierSchema identifierSchema;

    private String tableName;
    private String indexName;
    private IndexSchema indexSchema;

    public TimelineMetaSchema(String tableName, TimelineIdentifierSchema identifierSchema) {
        this.tableName = tableName;
        this.identifierSchema = identifierSchema;
    }

    public TimelineMetaSchema withIndex(String indexName, IndexSchema metaIndex) {
        this.indexName = indexName;
        this.indexSchema = metaIndex;
        return this;
    }

    public boolean hasMetaIndex() {
        return indexName != null && indexSchema != null;
    }

    public TimelineIdentifierSchema getIdentifierSchema() {
        return identifierSchema;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public IndexSchema getIndexSchema() {
        return indexSchema;
    }
}
