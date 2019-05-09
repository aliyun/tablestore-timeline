package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMessage;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestServiceClose {
    private static ServiceWrapper wrapper = null;
    private static TimelineStore storeTableService = null;
    private static TimelineStore syncTableService = null;
    private static SyncClient syncClient = null;

    @BeforeClass
    public static void setUp() throws Exception {
        wrapper = ServiceWrapper.newInstance();

        syncClient = wrapper.getSyncClient();
        storeTableService = wrapper.getStoreTableStore();
        syncTableService = wrapper.getSyncTableStore();

        storeTableService.prepareTables();
        syncTableService.prepareTables();

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();

        TimelineQueue storeTimelineQueue = storeTableService.createTimelineQueue(identifier);
        TimelineQueue syncTimelineQueue = syncTableService.createTimelineQueue(identifier);

        for (int i = 0; i < 1000; i++) {
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "message" + i);
            storeTimelineQueue.batchStore(message);
            syncTimelineQueue.batchStore(i, message);
        }

    }

    @AfterClass
    public static void after() throws Exception {
        storeTableService.dropAllTables();
        syncTableService.dropAllTables();

        syncClient.shutdown();
    }

    @Test
    public void testShutDown() {
        try {
            storeTableService.flush();
            storeTableService.close();
        } catch (Exception e) {
            fail();
        }
        try {
            syncTableService.flush();
            syncTableService.close();
        } catch (Exception e) {
            fail();
        }

    }
}
