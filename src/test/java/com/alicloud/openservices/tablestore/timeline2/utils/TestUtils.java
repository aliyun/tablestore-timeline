package com.alicloud.openservices.tablestore.timeline2.utils;

import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryType;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.FieldCondition;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import org.junit.Test;


import java.util.Arrays;
import java.util.List;

import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.field;
import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.and;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestUtils {
    @Test
    public void testSearchParameterToSearchQuery() {
        FieldCondition condition = and(
                field("group_name").equals("group_1"),
                field("create_time").greaterThan(1555163791285L)
        );

        SearchQuery searchQuery = Utils.toSearchQuery(
                new SearchParameter(condition)
                    .limit(5)
                    .calculateTotalCount()
        );

        assertEquals(5, searchQuery.getLimit().intValue());
        assertEquals(true, searchQuery.isGetTotalCount());
        assertEquals(QueryType.QueryType_BoolQuery, searchQuery.getQuery().getQueryType());
        assertEquals(2, ((BoolQuery)searchQuery.getQuery()).getMustQueries().size());
        assertEquals(QueryType.QueryType_TermQuery, ((BoolQuery)searchQuery.getQuery()).getMustQueries().get(0).getQueryType());
        assertEquals("group_name", ((TermQuery)((BoolQuery)searchQuery.getQuery()).getMustQueries().get(0)).getFieldName());
        assertEquals("group_1", ((TermQuery)((BoolQuery)searchQuery.getQuery()).getMustQueries().get(0)).getTerm().asString());
        assertEquals(QueryType.QueryType_RangeQuery, ((BoolQuery)searchQuery.getQuery()).getMustQueries().get(1).getQueryType());
    }

    @Test
    public void testTimelineMetaToRow() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField(new PrimaryKeyColumn("long", PrimaryKeyValue.fromLong(100)))
                .build();

        TimelineIdentifier identifier2 = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField(new PrimaryKeyColumn("long", PrimaryKeyValue.fromLong(100)))
                .build();

        TimelineMeta meta = new TimelineMeta(identifier)
                .setField("create_time", 123123123);

        Row row = Utils.metaToRow(meta);

        assertEquals(identifier2, meta.getIdentifier());
        assertEquals("group_a", row.getPrimaryKey().getPrimaryKeyColumn("timelineId").getValue().asString());
        assertTrue(
                row.getPrimaryKey().getPrimaryKeyColumn(1).equals(
                        new PrimaryKeyColumn("long", PrimaryKeyValue.fromLong(100))
                )
        );
        assertEquals(1, row.getColumns().length);
        assertEquals("create_time", row.getColumns()[0].getName());
        assertEquals(123123123, row.getColumns()[0].getValue().asLong());
    }

    @Test
    public void testMessageToRowPutChange() {
        TimelineMessage message = new TimelineMessage()
                .setField(new Column("content", ColumnValue.fromString("hello tablestore")));

        PrimaryKey primaryKey = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("timelineId", PrimaryKeyValue.fromString("group_a"))
                .addPrimaryKeyColumn("sequenceId", PrimaryKeyValue.fromLong(1000))
                .build();

        RowPutChange rowPutChange = Utils.messageToRowPutChange("tableName", primaryKey, message);

        assertTrue(rowPutChange.getPrimaryKey().equals(primaryKey));
        assertEquals("tableName", rowPutChange.getTableName());
        assertEquals(1, rowPutChange.getColumnsToPut().size());

        assertEquals("content", rowPutChange.getColumnsToPut().get(0).getName());
        assertEquals(ColumnValue.fromString("hello tablestore"), rowPutChange.getColumnsToPut().get(0).getValue());
    }

    @Test
    public void testIdentifierToPrimaryKeyWithSequenceId() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField(new PrimaryKeyColumn("long", PrimaryKeyValue.fromLong(100)))
                .build();

        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(
                identifier, "sequenceId", -1, true
        );

        assertEquals("group_a", primaryKey.getPrimaryKeyColumn("timelineId").getValue().asString());
        assertEquals(PrimaryKeyValue.AUTO_INCREMENT, primaryKey.getPrimaryKeyColumn("sequenceId").getValue());
        assertEquals(100, primaryKey.getPrimaryKeyColumn("long").getValue().asLong());

        PrimaryKey primaryKey2 = Utils.identifierToPrimaryKeyWithSequenceId(
                identifier, "sequenceId", 10000L, false
        );
        assertEquals(10000L, primaryKey2.getPrimaryKeyColumn("sequenceId").getValue().asLong());

    }

    @Test
    public void testIdentifierToPrimaryKey() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField(new PrimaryKeyColumn("long", PrimaryKeyValue.fromLong(100)))
                .build();

        PrimaryKey primaryKey = Utils.identifierToPrimaryKey(identifier);
        assertEquals("group_a", primaryKey.getPrimaryKeyColumn("timelineId").getValue().asString());
        assertEquals(100, primaryKey.getPrimaryKeyColumn("long").getValue().asLong());
    }

    @Test
    public void testPrimaryKeyToIdentifier() {
        TimelineIdentifier identifier = Utils.primaryKeyToIdentifier(
                new TimelineIdentifierSchema.Builder()
                        .addStringField("timelineId")
                        .addLongField("long")
                        .addBinaryField("bytes")
                        .build(),
                PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("timelineId", PrimaryKeyValue.fromString("group_a"))
                        .addPrimaryKeyColumn("long", PrimaryKeyValue.fromLong(1000))
                        .addPrimaryKeyColumn("bytes", PrimaryKeyValue.fromBinary(new byte[]{1, 0, 0, 8, 6}))
                        .build()
        );

        TimelineIdentifier identifier1 = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField(new PrimaryKeyColumn("long", PrimaryKeyValue.fromLong(1000)))
                .addField("bytes", new byte[]{1, 0, 0, 8, 6})
                .build();

        assertTrue(identifier.equals(identifier1));
        assertTrue(
                Bytes.equals(
                        new byte[]{1, 0, 0, 8, 6},
                        identifier.getField(2).getValue().asBinary()
                )
        );
        assertEquals("timelineId", identifier.getField(0).getName());
        assertEquals("group_a", identifier.getField(0).getValue().asString());
        assertEquals(3, identifier.getFields().size());

        try {
            Utils.primaryKeyToIdentifier(
                    new TimelineIdentifierSchema.Builder()
                            .addStringField("timelineId")
                            .build(),
                    PrimaryKeyBuilder.createPrimaryKeyBuilder()
                            .addPrimaryKeyColumn("differentTimelineId", PrimaryKeyValue.fromString("group_a"))
                            .build()
            );
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("Identifier schema not match primary key schema.", e.getMessage());
        }
    }

    @Test
    public void testRowToMeta() {
        TimelineIdentifierSchema schema = new TimelineIdentifierSchema.Builder()
                .addStringField("timelineId")
                .addLongField("long")
                .build();

        Row row = new Row(
                PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("timelineId", PrimaryKeyValue.fromString("group_a"))
                        .addPrimaryKeyColumn("long", PrimaryKeyValue.fromLong(10000))
                        .build(),
                new Column[]{ new Column("groupName", ColumnValue.fromString("tablestore"))}
        );

        TimelineMeta meta = Utils.rowToMeta(schema, row);

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 10000)
                .build();

        assertTrue(meta.getIdentifier().equals(identifier));
        assertEquals("tablestore", meta.getString("groupName"));

        meta = Utils.rowToMeta(schema, null);
        assertNull(meta);
    }

    @Test
    public void testRowToTimelineEntry() {
        Row row = new Row(
                PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("timelineId", PrimaryKeyValue.fromString("group_a"))
                        .addPrimaryKeyColumn("sequenceIdColumnName", PrimaryKeyValue.fromLong(10000))
                        .build(),
                new Column[]{
                        new Column("message_id", ColumnValue.fromString("message_1")),
                        new Column("content", ColumnValue.fromString("hello TableStore"))
                }
        );

        TimelineSchema schema = new TimelineSchema(
                "tableName",
                new TimelineIdentifierSchema.Builder()
                        .addStringField("timelineId")
                        .build()
        ).setSequenceIdColumnName("sequenceIdColumnName");

        TimelineEntry entry = Utils.rowToTimelineEntry(schema, row);

        assertEquals(10000, entry.getSequenceID());
        assertEquals("hello TableStore", entry.getMessage().getString("content"));

        entry = Utils.rowToTimelineEntry(schema, null);
        assertNull(entry);
    }

    @Test
    public void testRowToTimelineEntryWithMessage() {
        TimelineSchema schema = new TimelineSchema(
                "tableName",
                new TimelineIdentifierSchema.Builder()
                        .addStringField("timelineId")
                        .build()
        ).setSequenceIdColumnName("sequenceIdColumnName");

        Row row = new Row(
                PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("timelineId", PrimaryKeyValue.fromString("group_a"))
                        .addPrimaryKeyColumn("sequenceIdColumnName", PrimaryKeyValue.fromLong(10000))
                        .build(),
                new Column[]{ new Column("text", ColumnValue.fromString("notUsed"))}
        );

        TimelineMessage message = new TimelineMessage()
                .setField("text", "TableStore");


        TimelineEntry entry = Utils.rowToTimelineEntryWithMessage(schema, row, message);
        assertEquals("TableStore", entry.getMessage().getString("text"));
        assertEquals(10000, entry.getSequenceID());
    }

    @Test
    public void testRowToTimelineEntryWithColumnList() {
        TimelineSchema schema = new TimelineSchema(
                "tableName",
                new TimelineIdentifierSchema.Builder()
                        .addStringField("timelineId")
                        .build()
        ).setSequenceIdColumnName("sequenceIdColumnName");

        Row row = new Row(
                PrimaryKeyBuilder.createPrimaryKeyBuilder()
                        .addPrimaryKeyColumn("timelineId", PrimaryKeyValue.fromString("group_a"))
                        .addPrimaryKeyColumn("sequenceIdColumnName", PrimaryKeyValue.fromLong(10000))
                        .build(),
                new Column[]{ new Column("text", ColumnValue.fromString("notUsed"))}
        );

        List<Column> columnList = Arrays.asList(
                new Column("text", ColumnValue.fromString("TableStore"))
        );

        TimelineEntry entry = Utils.rowToTimelineEntryWithColumnList(schema, row, columnList);
        assertEquals("TableStore", entry.getMessage().getString("text"));
        assertEquals(10000, entry.getSequenceID());
    }
}