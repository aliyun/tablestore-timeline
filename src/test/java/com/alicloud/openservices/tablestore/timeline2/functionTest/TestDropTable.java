package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMessage;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMeta;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestDropTable {
    private static ServiceWrapper wrapper = null;
    private static TimelineMetaStore timelineMetaStore = null;
    private static TimelineStore storeTableService = null;
    private static TimelineStore syncTableService = null;
    private static SyncClient syncClient = null;

    @BeforeClass
    public static void setUp() throws Exception {
        wrapper = ServiceWrapper.newInstance();

        syncClient = wrapper.getSyncClient();

        timelineMetaStore = wrapper.getMetaStore();
        storeTableService = wrapper.getStoreTableStore();
    }

    @AfterClass
    public static void after() throws Exception {
        try {
            timelineMetaStore.dropAllTables();
        } catch (Exception e) {
        }
        try {
            storeTableService.dropAllTables();
        } catch (Exception e) {
        }
    }

    @Test
    public void testDropTables() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();

        TimelineMeta meta = new TimelineMeta(identifier)
                .setField("groupName", "group");

        TimelineMessage message = new TimelineMessage()
                .setField("text", "TableStore");

        timelineMetaStore.prepareTables();
        storeTableService.prepareTables();

        timelineMetaStore.insert(meta);
        TimelineQueue storeTable = storeTableService.createTimelineQueue(identifier);
        storeTable.store(message);

        TimelineMeta group = timelineMetaStore.read(identifier);
        TimelineEntry entry = storeTable.getLatestTimelineEntry();
        assertEquals("group", group.getString("groupName"));
        assertEquals("TableStore", entry.getMessage().getString("text"));

        timelineMetaStore.close();
        timelineMetaStore.dropAllTables();
        storeTableService.flush();
        storeTableService.close();
        storeTableService.dropAllTables();

        try {
            timelineMetaStore.read(identifier);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }


        try {
            storeTable.getLatestTimelineEntry();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

    }
}
