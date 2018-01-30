package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.utils.Utils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUtils {
    @Test
    public void testPutRowToTimelineEntry() {
        Response meta = new Response();
        ConsumedCapacity consumedCapacity = new ConsumedCapacity(new CapacityUnit());
        String timelineID = "t_1";
        Long sequenceID = 10001L;
        PrimaryKeyColumn firstPK = new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(timelineID));
        PrimaryKeyColumn secondPK = new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(sequenceID));
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().
                addPrimaryKeyColumn(firstPK).addPrimaryKeyColumn(secondPK).build();
        Row row = new Row(pk, new Column[0]);

        PutRowResponse response = new PutRowResponse(meta, row, consumedCapacity);
        IMessage message = new StringMessage("content");

        TimelineEntry entry = Utils.toTimelineEntry(response, message);
        assertEquals(sequenceID, entry.getSequenceID());
        assertEquals(message, entry.getMessage());
    }

    @Test
    public void TestGetRowToTimelineEntry_OnlyContent() {
        DistributeTimelineConfig config = new DistributeTimelineConfig("", "",
                "", "", "");
        String timelineID = "t_1";
        Long sequenceID = 10001L;
        PrimaryKeyColumn firstPK = new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(timelineID));
        PrimaryKeyColumn secondPK = new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(sequenceID));
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().
                addPrimaryKeyColumn(firstPK).addPrimaryKeyColumn(secondPK).build();
        Column[] columns = new Column[5];
        columns[0] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix() + "10000",
                ColumnValue.fromBinary("1".getBytes()));
        columns[1] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix() + "10001",
                ColumnValue.fromBinary("2".getBytes()));
        columns[2] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix() + "10002",
                ColumnValue.fromBinary("3".getBytes()));
        columns[3] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageIDColumnNameSuffix(),
                ColumnValue.fromString("10000000001"));
        columns[4] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getColumnNameOfMessageCrc32Suffix(),
                ColumnValue.fromLong(Utils.crc32("123".getBytes())));
        Row row = new Row(pk, columns);

        TimelineEntry entry = Utils.toTimelineEntry(row, config);

        assertEquals(sequenceID, entry.getSequenceID());
        assertEquals("123", new String(entry.getMessage().serialize()));
        assertEquals("10000000001", entry.getMessage().getMessageID());
    }

    @Test
    public void TestGetRowToTimelineEntry_HasAttributeColumn() {
        DistributeTimelineConfig config = new DistributeTimelineConfig("", "",
                "", "", "");
        String timelineID = "t_1";
        Long sequenceID = 10001L;
        PrimaryKeyColumn firstPK = new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(timelineID));
        PrimaryKeyColumn secondPK = new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(sequenceID));
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().
                addPrimaryKeyColumn(firstPK).addPrimaryKeyColumn(secondPK).build();
        Column[] columns = new Column[8];
        columns[0] = new Column("age", ColumnValue.fromString("14"));
        columns[1] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageIDColumnNameSuffix(),
                ColumnValue.fromString("10000000001"));
        columns[2] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix() + "10000",
                ColumnValue.fromBinary("1".getBytes()));
        columns[3] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix() + "10001",
                ColumnValue.fromBinary("2".getBytes()));
        columns[4] = new Column("address", ColumnValue.fromString("hangzhou"));
        columns[5] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix() + "10002",
                ColumnValue.fromBinary("3".getBytes()));
        columns[6] = new Column("phone", ColumnValue.fromString("13086666666"));
        columns[7] = new Column(Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getColumnNameOfMessageCrc32Suffix(),
                ColumnValue.fromLong(Utils.crc32("123".getBytes())));

        Row row = new Row(pk, columns);

        TimelineEntry entry = Utils.toTimelineEntry(row, config);

        assertEquals(sequenceID, entry.getSequenceID());
        assertEquals("123", new String(entry.getMessage().serialize()));
        assertEquals("10000000001", entry.getMessage().getMessageID());
        assertEquals(3, entry.getMessage().getAttributes().size());
        assertEquals("14", entry.getMessage().getAttributes().get("age"));
        assertEquals("hangzhou", entry.getMessage().getAttributes().get("address"));
        assertEquals("13086666666", entry.getMessage().getAttributes().get("phone"));
    }

    @Test
    public void testGetLocalIP() {
        String ip = Utils.getLocalIP();
        assertTrue(!ip.equals("127.0.0.1"));
        assertTrue(ip.length() > 0);
    }

    @Test
    public void testGetProcessID() {
        assertTrue(Utils.getProcessID().length() > 0);
    }
}
