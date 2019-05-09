package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExceptionDropTables {
    private static ServiceWrapper wrapper = null;
    private static TimelineMetaStore timelineMetaStore = null;
    private static TimelineStore storeTableService = null;
    private static TimelineStore syncTableService = null;
    private static SyncClient syncClient = null;
    private static TimelineIdentifier identifier = null;

    @BeforeClass
    public static void setUp() throws Exception {
        wrapper = ServiceWrapper.newInstance();

        syncClient = wrapper.getSyncClient();

        timelineMetaStore = wrapper.getMetaStore();
        storeTableService = wrapper.getStoreTableStore();
        syncTableService = wrapper.getSyncTableStore();
        identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();
    }

    @AfterClass
    public static void after() throws Exception {

    }

    @Test
    public void testDropTablesException() {
        try {
            timelineMetaStore.dropAllTables();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        try {
            storeTableService.dropAllTables();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        try {
            syncTableService.dropAllTables();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }
    }
}
