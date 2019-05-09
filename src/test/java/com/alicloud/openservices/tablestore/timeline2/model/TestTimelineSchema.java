package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTimelineSchema {
    @Test
    public void testBasic() {
        TimelineIdentifierSchema identifierSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("pk0")
                .addStringField("pk1").build();

        String tableName = "table1";
        TimelineSchema timelineSchema = new TimelineSchema(tableName, identifierSchema);
        assertEquals(timelineSchema.isAutoGenerateSeqId(), true);
        assertEquals(timelineSchema.hasDataIndex(), false);
        assertEquals(timelineSchema.getTableName(), tableName);
        assertArrayEquals(timelineSchema.getIdentifierSchema().getKeys().toArray(), identifierSchema.getKeys().toArray());

        timelineSchema.manualSetSeqId();
        assertEquals(timelineSchema.isAutoGenerateSeqId(), false);
        timelineSchema.autoGenerateSeqId();
        assertEquals(timelineSchema.isAutoGenerateSeqId(), true);

        assertEquals(timelineSchema.getTimeToLive(), -1);
        timelineSchema.setTimeToLive(86400);
        assertEquals(timelineSchema.getTimeToLive(), 86400);

        String indexName = "index1";
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.addFieldSchema(new FieldSchema("pk0", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true));
        indexSchema.addFieldSchema(new FieldSchema("pk1", FieldType.TEXT).setIndex(true));

        timelineSchema.withIndex(indexName, indexSchema);
        assertEquals(timelineSchema.getIndexName(), indexName);
        assertArrayEquals(timelineSchema.getIndexSchema().getFieldSchemas().toArray(), indexSchema.getFieldSchemas().toArray());

        assertEquals(timelineSchema.getSequenceIdColumnName(), TimelineSchema.SEQUENCE_ID_COLUMN_NAME);
        timelineSchema.setSequenceIdColumnName("sid");
        assertEquals(timelineSchema.getSequenceIdColumnName(), "sid");

        long cpuCount = Runtime.getRuntime().availableProcessors();
        assertTrue(timelineSchema.getCallbackExecuteThreads() > 0);
        assertTrue(cpuCount + 1 == timelineSchema.getMaxCallbackExecuteThreads());
        assertTrue(timelineSchema.getMaxCallbackExecuteThreads() >= timelineSchema.getCallbackExecuteThreads());

        timelineSchema.setCallbackExecuteThreads(20);
        assertTrue(timelineSchema.getMaxCallbackExecuteThreads() == 20);
        assertTrue(timelineSchema.getMaxCallbackExecuteThreads() >= timelineSchema.getCallbackExecuteThreads());
    }
}
