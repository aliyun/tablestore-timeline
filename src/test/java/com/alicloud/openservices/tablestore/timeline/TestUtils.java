package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.model.*;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
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
        columns[0] = new Column(config.getMessageContentPrefix() + "10000", ColumnValue.fromBinary("1".getBytes()));
        columns[1] = new Column(config.getMessageContentPrefix() + "10001", ColumnValue.fromBinary("2".getBytes()));
        columns[2] = new Column(config.getMessageContentPrefix() + "10002", ColumnValue.fromBinary("3".getBytes()));
        columns[3] = new Column(config.getMessageIDColumnName(), ColumnValue.fromString("10000000001"));
        columns[4] = new Column(config.getColumnNameOfMessageCrc32(), ColumnValue.fromLong(Utils.crc32("123".getBytes())));
        Row row = new Row(pk, columns);

        TimelineEntry entry = Utils.toTimelineEntry(row, config);

        assertEquals(sequenceID, entry.getSequenceID());
        assertEquals("123", new String(entry.getMessage().serialize()));
        assertEquals("10000000001", entry.getMessage().getMessageID());
    }

    @Test
    public void TestGetRowToTimelineEntry_HasOtherColumn() {
        DistributeTimelineConfig config = new DistributeTimelineConfig("", "",
                "", "", "");
        String timelineID = "t_1";
        Long sequenceID = 10001L;
        PrimaryKeyColumn firstPK = new PrimaryKeyColumn("pk1", PrimaryKeyValue.fromString(timelineID));
        PrimaryKeyColumn secondPK = new PrimaryKeyColumn("pk2", PrimaryKeyValue.fromLong(sequenceID));
        PrimaryKey pk = PrimaryKeyBuilder.createPrimaryKeyBuilder().
                addPrimaryKeyColumn(firstPK).addPrimaryKeyColumn(secondPK).build();
        Column[] columns = new Column[8];
        columns[0] = new Column("00", ColumnValue.fromBinary("hangzhou".getBytes()));
        columns[1] = new Column(config.getMessageIDColumnName(), ColumnValue.fromString("10000000001"));
        columns[2] = new Column(config.getMessageContentPrefix() + "10000", ColumnValue.fromBinary("1".getBytes()));
        columns[3] = new Column(config.getMessageContentPrefix() + "10001", ColumnValue.fromBinary("2".getBytes()));
        columns[4] = new Column("10003", ColumnValue.fromBinary("2".getBytes()));
        columns[5] = new Column(config.getMessageContentPrefix() + "10002", ColumnValue.fromBinary("3".getBytes()));
        columns[6] = new Column("99", ColumnValue.fromBinary("chengdu".getBytes()));
        columns[7] = new Column(config.getColumnNameOfMessageCrc32(), ColumnValue.fromLong(Utils.crc32("123".getBytes())));

        Row row = new Row(pk, columns);

        TimelineEntry entry = Utils.toTimelineEntry(row, config);

        assertEquals(sequenceID, entry.getSequenceID());
        assertEquals("123", new String(entry.getMessage().serialize()));
        assertEquals("10000000001", entry.getMessage().getMessageID());
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
