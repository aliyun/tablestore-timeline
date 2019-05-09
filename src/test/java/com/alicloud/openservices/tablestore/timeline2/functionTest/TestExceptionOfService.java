package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.MatchAllQuery;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMessage;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMeta;
import com.alicloud.openservices.tablestore.timeline2.query.ScanParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.Future;

import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.field;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestExceptionOfService {
    private static ServiceWrapper wrapper = null;
    private static TimelineMetaStore timelineMetaStore = null;
    private static TimelineStore storeTableService = null;
    private static TimelineStore syncTableService = null;
    private static SyncClient syncClient = null;
    private static TimelineIdentifier identifier = null;
    private static TimelineQueue storeTable = null;
    private static TimelineQueue syncTable = null;

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
        storeTableService.prepareTables();
        syncTableService.prepareTables();

        storeTable = storeTableService.createTimelineQueue(identifier);
        syncTable = syncTableService.createTimelineQueue(identifier);

        storeTableService.dropAllTables();
        syncTableService.dropAllTables();
    }

    @AfterClass
    public static void after() throws Exception {

    }


    @Test
    public void testTimelineException() {
        TimelineMessage message = new TimelineMessage();
        message.setField("text", "TableStore");

        try {
            storeTable.getLatestTimelineEntry();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        try {
            storeTable.store(message);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            syncTable.store(1, message);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            storeTable.delete(0);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            storeTable.get(0);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        try {
            storeTable.update(1, message);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            storeTable.scan(new ScanParameter().scanBackward(Long.MAX_VALUE, 0));
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        Future<TimelineEntry> storeFuture1 = storeTable.storeAsync(message, null);
        try {
            storeFuture1.cancel(false);
            storeFuture1.get();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        Future<TimelineEntry> storeFuture2 = syncTable.storeAsync(1, message, null);
        try {
            storeFuture2.cancel(false);
            storeFuture2.get();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        Future<TimelineEntry> future1 = storeTable.batchStore(message);
        try {
            future1.cancel(false);
            future1.get();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        Future<TimelineEntry> future2 = syncTable.batchStore(1, message);
        try {
            future2.cancel(false);
            future2.get();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        Future<TimelineEntry> updateFuture1 = storeTable.updateAsync(1, message, null);
        try {
            updateFuture1.cancel(true);
            updateFuture1.get();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            storeTable.close();
            syncTable.close();
        } catch (Exception e) {
            assertNull(e);//should not throw exception
        }
    }

    @Test
    public void testMetaServiceException() {
        TimelineMeta meta = new TimelineMeta(identifier)
                .setField("groupName", "group");

        try {
            timelineMetaStore.insert(meta);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            timelineMetaStore.delete(identifier);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSObjectNotExist", e.getMessage());
        }

        try {
            timelineMetaStore.read(identifier);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        MatchAllQuery query = new MatchAllQuery();

        SearchQuery searchQuery = new SearchQuery()
                .setQuery(query)
                .setLimit(1);
        try {
            timelineMetaStore.search(searchQuery);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        SearchParameter parameter = new SearchParameter(
                field("text").equals(1)
        );
        try {
            timelineMetaStore.search(parameter);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        try {
            timelineMetaStore.dropAllTables();
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testTimelineServiceException() {
        try {
            storeTableService.createTimelineQueue(null);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("Identifier should not be null.", e.getMessage());
        }

        MatchAllQuery query = new MatchAllQuery();
        SearchQuery searchQuery = new SearchQuery()
                .setQuery(query)
                .setLimit(1);

        try {
            storeTableService.search(searchQuery);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        SearchParameter parameter = new SearchParameter(
                field("text").equals(1)
        );
        try {
            storeTableService.search(parameter);
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
