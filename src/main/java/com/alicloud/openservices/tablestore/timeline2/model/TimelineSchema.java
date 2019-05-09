package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.writer.WriterConfig;

public class TimelineSchema {
    public static final String SEQUENCE_ID_COLUMN_NAME = "sequence_id";

    public enum SequenceIdGeneration {
        /**
         * Auto generate sequence id, use TableStore's auto-increment column.
         */
        AUTO_INCREMENT,

        /**
         * Manual set sequence id, should be unique and incremental.
         */
        MANUAL
    }

    private String tableName;
    private String indexName;
    private TimelineIdentifierSchema identifierSchema;
    private SequenceIdGeneration sequenceIdGeneration = SequenceIdGeneration.AUTO_INCREMENT;
    private String sequenceIdColumnName = SEQUENCE_ID_COLUMN_NAME;
    private int ttl = -1;

    private IndexSchema indexSchema;

    private int maxCallbackExecuteThreads = Runtime.getRuntime().availableProcessors() + 1;
    private int callbackExecuteThreads = maxCallbackExecuteThreads / 2;
    private WriterConfig writerConfig;

    public TimelineSchema(String tableName, TimelineIdentifierSchema identifierSchema) {
        this.tableName = tableName;
        this.identifierSchema = identifierSchema;
        this.writerConfig = new WriterConfig();
    }

    public TimelineSchema autoGenerateSeqId() {
        this.sequenceIdGeneration = SequenceIdGeneration.AUTO_INCREMENT;
        return this;
    }

    public TimelineSchema manualSetSeqId() {
        this.sequenceIdGeneration = SequenceIdGeneration.MANUAL;
        return this;
    }

    public TimelineSchema withIndex(String indexName, IndexSchema dataIndex) {
        this.indexName = indexName;
        this.indexSchema = dataIndex;
        return this;
    }

    public TimelineSchema setTimeToLive(int ttl) {
        this.ttl = ttl;
        return this;
    }

    public TimelineSchema setSequenceIdColumnName(String sequenceIdColumnName) {
        this.sequenceIdColumnName = sequenceIdColumnName;
        return this;
    }

    public TimelineSchema withWriterConfig(WriterConfig writerConfig) {
        this.writerConfig = writerConfig;
        return this;
    }

    public TimelineSchema setCallbackExecuteThreads(int callbackExecuteThreads) {
        this.callbackExecuteThreads = callbackExecuteThreads;
        if (callbackExecuteThreads > maxCallbackExecuteThreads) {
            maxCallbackExecuteThreads = callbackExecuteThreads;
        }
        return this;
    }

    public WriterConfig getWriterConfig() {
        return writerConfig;
    }

    public String getSequenceIdColumnName() {
        return sequenceIdColumnName;
    }

    public int getTimeToLive() {
        return ttl;
    }

    public boolean hasDataIndex() {
        return indexName != null && indexSchema != null;
    }

    public TimelineIdentifierSchema getIdentifierSchema() {
        return identifierSchema;
    }

    public boolean isAutoGenerateSeqId() {
        return sequenceIdGeneration == SequenceIdGeneration.AUTO_INCREMENT;
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


    public int getMaxCallbackExecuteThreads() {
        return maxCallbackExecuteThreads;
    }

    public int getCallbackExecuteThreads() {
        return callbackExecuteThreads;
    }
}
