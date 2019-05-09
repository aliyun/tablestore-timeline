package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.timeline2.*;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.ScanParameter;
import org.junit.*;

import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.alicloud.openservices.tablestore.timeline2.common.Cons.LONG_INVALID_COLUME_NAME;
import static org.junit.Assert.*;

public class TestTimeline {
    private static ServiceWrapper wrapper = null;
    private static TimelineMetaStore metaService = null;
    private static TimelineStore storeTableService = null;
    private static TimelineStore syncTableService = null;
    private static SyncClient syncClient = null;

    @BeforeClass
    public static void setUp() throws Exception {
        wrapper = ServiceWrapper.newInstance();

        syncClient = wrapper.getSyncClient();
        metaService = wrapper.getMetaStore();
        storeTableService = wrapper.getStoreTableStore();
        syncTableService = wrapper.getSyncTableStore();

        metaService.prepareTables();
        storeTableService.prepareTables();
        syncTableService.prepareTables();

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();

        TimelineMeta insertGroup = new TimelineMeta(identifier)
                .setField("groupName", "tablestore")
                .setField("createTime", 1);

        metaService.insert(insertGroup);
    }

    @AfterClass
    public static void after() throws Exception {
        metaService.dropAllTables();
        storeTableService.dropAllTables();
        syncTableService.dropAllTables();

        wrapper.shutdown();
    }

    @Test
    public void testGetIdentifier() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();
        TimelineQueue group =  storeTableService.createTimelineQueue(identifier);

        assertTrue(identifier.equals(group.getIdentifier()));
    }

    @Test
    public void testTimelineAutoSeqId() {
        String[] groupMember = new String[]{"user_a", "user_b"};

        TimelineQueue groupTimelineQueue = storeTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "group_a")
                        .addField("long", 1000L)
                        .build()
        );

        TimelineMessage tm = new TimelineMessage()
                .setField("text", "hello tablestore")
                .setField("receivers", groupMember)
                .setField("timestamp", 1)
                .setField("boolean", true);
        groupTimelineQueue.store(tm);

        /**
         * test scan store messages;
         * */
        Iterator<TimelineEntry> groupMessages = groupTimelineQueue.scan(
                new ScanParameter()
                        .scanForward(0)
                        .maxCount(10)
        );
        assertTrue(groupMessages.hasNext());
        try {
            groupMessages.remove();
        } catch (Exception e) {
            assertTrue(e instanceof ClientException);
            assertEquals("RowIterator do not support remove().", e.getMessage());
        }

        TimelineEntry groupEntry = groupMessages.next();
        assertEquals("hello tablestore", groupEntry.getMessage().getString("text"));
        assertEquals(1, groupEntry.getMessage().getLong("timestamp"));
        assertTrue(groupEntry.getMessage().getBoolean("boolean"));

        /**
         * delete store message;
         * */
        groupTimelineQueue.delete(groupEntry.getSequenceID());
        TimelineEntry deletedGroupEntry = groupTimelineQueue.get(groupEntry.getSequenceID());
        assertNull(deletedGroupEntry);

        try {
            tm.setField(LONG_INVALID_COLUME_NAME, "invalid");
            groupTimelineQueue.store(tm);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testTimelineManualSeqId() {

        TimelineQueue userTimelineQueue = syncTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "user_a")
                        .addField("long", 1000L)
                        .build());

        TimelineMessage tm1 = new TimelineMessage()
                .setField("text", "hello tablestore2")
                .setField("timestamp", 1);
        userTimelineQueue.store(1, tm1);

        TimelineMessage tm2 = new TimelineMessage()
                .setField("text", "hello tablestore2")
                .setField("timestamp", 2);
        userTimelineQueue.store(2, tm2);



        /**
         * test scan sync messages;
         * */
        Iterator<TimelineEntry> userMessages = userTimelineQueue.scan(
                new ScanParameter()
                        .scanBackward(Long.MAX_VALUE)
                        .maxCount(3)
        );
        assertTrue(userMessages.hasNext());

        TimelineEntry userEntry = userMessages.next();
        assertEquals("hello tablestore2", userEntry.getMessage().getString("text"));
        assertEquals(2, userEntry.getMessage().getLong("timestamp"));
        assertEquals(2, userEntry.getSequenceID());
        userTimelineQueue.delete(userEntry.getSequenceID());

        /**
         * delete sync message;
         * */
        userTimelineQueue.delete(1);
        TimelineEntry deletedUserEntry = userTimelineQueue.get(1);
        assertNull(deletedUserEntry);

        try {
            tm1.setField(LONG_INVALID_COLUME_NAME, "invalid");
            userTimelineQueue.store(10, tm1);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testStoreAsyncAutoSeqId() {
        TimelineQueue autoGenerateSeqIdTimelineQueue = storeTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testStoreAsyncAuto")
                        .addField("long", 1000L)
                        .build());

        for (int i = 0; i < 10; i++) {
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "testStoreAsyncAuto");

            TimelineCallback callback = new TimelineCallback() {
                @Override
                public void onCompleted(TimelineIdentifier identifier, TimelineMessage timelineMessage, TimelineEntry timelineEntry) {
                    assertEquals("testStoreAsyncAuto", timelineEntry.getMessage().getString("text"));
                }

                @Override
                public void onFailed(TimelineIdentifier identifier, TimelineMessage timelineMessage, Exception ex) {
                    assertEquals("testStoreAsyncAuto", timelineMessage.getString("text"));
                    System.out.println("StoreAsync Callback Failed:" + ex.getMessage());
                }
            };

            Future<TimelineEntry> future = autoGenerateSeqIdTimelineQueue.storeAsync(message, callback);
            try {
                TimelineEntry entry = future.get();
                assertEquals("testStoreAsyncAuto", entry.getMessage().getString("text"));
                assertTrue(future.isDone());
                assertFalse(future.isCancelled());
            } catch (Exception e) {
                assertNull(e);//should not throw exception
                e.printStackTrace();
            }
        }

        Iterator<TimelineEntry> iterator = autoGenerateSeqIdTimelineQueue.scan(
                new ScanParameter()
                        .maxCount(10)
                        .scanForwardTo(Long.MAX_VALUE)
                        .withFilter(
                                new SingleColumnValueFilter(
                                        "text",
                                        SingleColumnValueFilter.CompareOperator.EQUAL,
                                        ColumnValue.fromString("testStoreAsyncAuto")
                                )
                        )
        );

        int counter = 0;
        while (iterator.hasNext()) {
            counter++;
            TimelineEntry entry = iterator.next();
            autoGenerateSeqIdTimelineQueue.delete(entry.getSequenceID());
        }
        assertEquals(10, counter);


    }

    @Test
    public void testStoreAsyncManualSeqId() {
        TimelineQueue manualSetSeqIdTimelineQueue = syncTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testStoreAsyncManual")
                        .addField("long", 1000L)
                        .build());

        for (int i = 0; i < 5; i++) {//test get();
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "testStoreAsyncManual");

            Future<TimelineEntry> future =  manualSetSeqIdTimelineQueue.storeAsync(i, message, null);
            try {
                TimelineEntry entry = future.get();
                assertTrue(future.isDone());
                assertFalse(future.isCancelled());
                assertEquals(i, entry.getSequenceID());
            } catch (Exception e) {
                fail();
            }
        }

        for (int i = 5; i < 10; i++) {//test get(timeout, unit);
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "testStoreAsyncManual");

            Future<TimelineEntry> future =  manualSetSeqIdTimelineQueue.storeAsync(i, message, null);
            try {
                TimelineEntry entry = future.get(1, TimeUnit.SECONDS);
                assertTrue(future.isDone());
                assertFalse(future.isCancelled());
                assertEquals(i, entry.getSequenceID());
            } catch (Exception e) {
                assertNull(e);//should not throw exception
                e.printStackTrace();
            }
        }

        Iterator<TimelineEntry> iterator = manualSetSeqIdTimelineQueue.scan(
                new ScanParameter()
                        .maxCount(10)
                        .scanBackwardTo(0)
                        .withFilter(
                                new SingleColumnValueFilter(
                                        "text",
                                        SingleColumnValueFilter.CompareOperator.EQUAL,
                                        ColumnValue.fromString("testStoreAsyncManual")
                                )
                        )
        );

        int counter = 9;
        while (iterator.hasNext()) {
            TimelineEntry entry = iterator.next();
            assertEquals(counter--, entry.getSequenceID());

            manualSetSeqIdTimelineQueue.delete(entry.getSequenceID());
        }
        assertEquals(0, counter);

    }

    @Test
    public void testBatchStoreAutoSeqId() {
        TimelineQueue autoGenerateSeqIdTimelineQueue = storeTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testBatchAuto")
                        .addField("long", 1000L)
                        .build());

        for (int i = 0; i < 10; i++) {
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "testBatchAuto");

            autoGenerateSeqIdTimelineQueue.batchStore(message);
        }
        autoGenerateSeqIdTimelineQueue.flush();
        ServiceWrapper.sleepForWriterOrAsync();

        Iterator<TimelineEntry> iterator = autoGenerateSeqIdTimelineQueue.scan(
                new ScanParameter()
                        .maxCount(10)
                        .scanForward(0, Long.MAX_VALUE)
                        .withFilter(
                                new SingleColumnValueFilter(
                                        "text",
                                        SingleColumnValueFilter.CompareOperator.EQUAL,
                                        ColumnValue.fromString("testBatchAuto")
                                )
                        )
        );

        int counter = 0;
        while (iterator.hasNext()) {
            TimelineEntry entry = iterator.next();
            counter++;
            autoGenerateSeqIdTimelineQueue.delete(entry.getSequenceID());
        }
        assertEquals(10, counter);
    }

    @Test
    public void testBatchStoreManualSeqId() {
        TimelineQueue manualSetSeqIdTimelineQueue = syncTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testBatchManual")
                        .addField("long", 1000L)
                        .build());

        for (int i = 0; i < 10; i++) {
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "testBatchManual");

            manualSetSeqIdTimelineQueue.batchStore(i, message);
        }
        manualSetSeqIdTimelineQueue.flush();
        ServiceWrapper.sleepForWriterOrAsync();

        Iterator<TimelineEntry> iterator = manualSetSeqIdTimelineQueue.scan(
                new ScanParameter()
                        .maxCount(10)
                        .scanBackward(Long.MAX_VALUE)
                        .withFilter(
                                new SingleColumnValueFilter(
                                        "text",
                                        SingleColumnValueFilter.CompareOperator.EQUAL,
                                        ColumnValue.fromString("testBatchManual")
                                )
                        )
        );

        int counter = 9;
        while (iterator.hasNext()) {
            TimelineEntry entry = iterator.next();
            assertEquals(counter--, entry.getSequenceID());

            manualSetSeqIdTimelineQueue.delete(entry.getSequenceID());
        }
        assertEquals(0, counter);

    }

    @Test
    public void testUpdateManualSeqId() {
        TimelineQueue timelineQueue = syncTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testUpdate")
                        .addField("long", 1000L)
                        .build());

        TimelineMessage message = new TimelineMessage()
                .setField("text", "testUpdate");
        timelineQueue.store(1, message);
        TimelineMessage readMessage = timelineQueue.get(1).getMessage();

        /**
         * test update by sync api
         * */
        readMessage.setField("text", "testUpdate");
        timelineQueue.update(1, readMessage);

        readMessage = timelineQueue.get(1).getMessage();
        assertEquals("testUpdate", readMessage.getString("text"));

        /**
         * test update by async api with get();
         * */
        readMessage.setField("text", "updateSync by get()");
        Future<TimelineEntry> future =  timelineQueue.updateAsync(1, readMessage, null);
        try {
            TimelineEntry entry = future.get();
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
            assertEquals("updateSync by get()", entry.getMessage().getString("text"));
        } catch (Exception e) {
            fail();
        }

        readMessage = timelineQueue.get(1).getMessage();
        assertEquals("updateSync by get()", readMessage.getString("text"));

        /**
         * test update by async api with get(timeout, unit);
         * */
        readMessage.setField("text", "updateAsync with get(timeout, unit)");
        future =  timelineQueue.updateAsync(1, readMessage, null);
        try {
            TimelineEntry entry = future.get(1, TimeUnit.SECONDS);
            assertEquals("updateAsync with get(timeout, unit)", entry.getMessage().getString("text"));
        } catch (Exception e) {
            fail();
        }

        readMessage = timelineQueue.get(1).getMessage();
        assertEquals("updateAsync with get(timeout, unit)", readMessage.getString("text"));


        timelineQueue.delete(1);

        /**
         * test update an not-exist timeline and succeed.
         * */
        timelineQueue.update(100000, readMessage);
        readMessage = timelineQueue.get(100000).getMessage();
        assertEquals("updateAsync with get(timeout, unit)", readMessage.getString("text"));

        timelineQueue.delete(100000);

        try {
            readMessage.setField(LONG_INVALID_COLUME_NAME, "invalid");
            timelineQueue.update(1111, readMessage);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testUpdateAutoSeqId() {
        TimelineQueue timelineQueue = storeTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testUpdateAutoSeqId")
                        .addField("long", 1000L)
                        .build());

        TimelineMessage message = new TimelineMessage()
                .setField("text", "testUpdateAutoSeqId");
        timelineQueue.store(message);

        TimelineEntry timelineEntry = timelineQueue.getLatestTimelineEntry();
        long sequenceId = timelineEntry.getSequenceID();
        TimelineMessage readMessage = timelineEntry.getMessage();

        TimelineCallback callback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage message, TimelineEntry timelineEntry) {

            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage message, Exception ex) {

            }
        };

        /**
         * test update by sync api
         * */
        readMessage.setField("text", "testUpdate");
        timelineQueue.update(sequenceId, readMessage);

        readMessage = timelineQueue.get(sequenceId).getMessage();
        assertEquals("testUpdate", readMessage.getString("text"));

        /**
         * test update by async api with get();
         * */
        readMessage.setField("text", "updateSync by get()");
        Future<TimelineEntry> future =  timelineQueue.updateAsync(sequenceId, readMessage, callback);
        try {
            TimelineEntry entry = future.get();
            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
            assertEquals("updateSync by get()", entry.getMessage().getString("text"));
        } catch (Exception e) {
            fail();
        }

        readMessage = timelineQueue.get(sequenceId).getMessage();
        assertEquals("updateSync by get()", readMessage.getString("text"));

        /**
         * test update by async api with get(timeout, unit);
         * */
        readMessage.setField("text", "updateAsync with get(timeout, unit)");
        future =  timelineQueue.updateAsync(sequenceId, readMessage, callback);
        try {
            TimelineEntry entry = future.get(1, TimeUnit.SECONDS);
            assertEquals("updateAsync with get(timeout, unit)", entry.getMessage().getString("text"));
        } catch (Exception e) {
            fail();
        }

        readMessage = timelineQueue.get(sequenceId).getMessage();
        assertEquals("updateAsync with get(timeout, unit)", readMessage.getString("text"));


        timelineQueue.delete(sequenceId);

        /**
         * test update an not-exist timeline and succeed.
         * */
        try {
            timelineQueue.update(1, message);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSConditionCheckFail", e.getMessage());
        }

        TimelineEntry nullMessage = timelineQueue.get(1);
        assertNull(nullMessage);

        try {
            timelineQueue.delete(100000);
        } catch (Exception e) {
            fail();
        }

        try {
            timelineQueue.update(1, readMessage);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSConditionCheckFail", e.getMessage());
        }
    }

    @Test
    public void testLatestTimelineEntry() {
        TimelineQueue timelineQueue = syncTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "testLatest")
                        .addField("long", 1000L)
                        .build());

        for (int i = 1; i <= 10; i++) {
            TimelineMessage message = new TimelineMessage()
                    .setField("text", "testBatchManual");
            timelineQueue.store(i, message);
        }

        for (int i = 10; i >= 1; i--) {
            timelineQueue.delete(i);

            TimelineEntry latestTimelineEntry = timelineQueue.getLatestTimelineEntry();
            if (latestTimelineEntry != null) {
                assertEquals(i - 1, latestTimelineEntry.getSequenceID());
                assertEquals(i - 1, timelineQueue.getLatestSequenceId());
            }
        }
    }

    @Test
    public void testBatchStoreAutoSedIdAsync() {
        final TimelineIdentifier originIdentifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "testBatchStoreAsync")
                .addField("long", 1000L)
                .build();

        TimelineQueue autoGenerateSeqIdTimelineQueue = storeTableService.createTimelineQueue(originIdentifier);

        final TimelineMessage message = new TimelineMessage()
                .setField("text", "testStoreAsyncAuto");

        TimelineCallback callback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage request, TimelineEntry timelineEntry) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncAuto", timelineEntry.getMessage().getString("text"));
                assertEquals("testStoreAsyncAuto", message.getString("text"));
            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage request, Exception ex) {
                assertEquals(originIdentifier, identifier);
                fail();
            }
        };

        Future<TimelineEntry> future = autoGenerateSeqIdTimelineQueue.batchStore(message, callback);
        try {
            TimelineEntry entry = future.get(10, TimeUnit.SECONDS);
            assertEquals("testStoreAsyncAuto", entry.getMessage().getString("text"));

            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testBatchStoreManualSedIdAsync() {
        final TimelineIdentifier originIdentifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "testBatchStoreAsync")
                .addField("long", 1000L)
                .build();
        TimelineQueue manualGenerateSeqIdTimelineQueue = syncTableService.createTimelineQueue(originIdentifier);

        final TimelineMessage message = new TimelineMessage()
                .setField("text", "testStoreAsyncManual");

        TimelineCallback callback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage timelineMessage, TimelineEntry timelineEntry) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncManual", timelineEntry.getMessage().getString("text"));
                assertEquals("testStoreAsyncManual", timelineMessage.getString("text"));
            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage timelineMessage, Exception ex) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncManual", timelineMessage.getString("text"));
                fail();
            }
        };

        Future<TimelineEntry> future = manualGenerateSeqIdTimelineQueue.batchStore(1, message, callback);
        try {
            TimelineEntry entry = future.get(10, TimeUnit.SECONDS);
            assertEquals("testStoreAsyncManual", entry.getMessage().getString("text"));

            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        } catch (Exception e) {
            fail();
        }
    }

    @Test
    public void testBatchStoreFailedCallback() {
        final TimelineIdentifier originIdentifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "testFailedCallback")
                .addField("long", 1000L)
                .build();

        TimelineQueue timelineQueue = syncTableService.createTimelineQueue(originIdentifier);

        final String invalidColumnName = LONG_INVALID_COLUME_NAME;
        final TimelineMessage message = new TimelineMessage()
                .setField(invalidColumnName, "testFailedCallback");

        TimelineCallback callback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage timelineMessage, TimelineEntry timelineEntry) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testFailedCallback", timelineEntry.getMessage().getString("text"));
                assertEquals("testFailedCallback", timelineMessage.getString("text"));
            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage timelineMessage, Exception e) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testFailedCallback", timelineMessage.getString(invalidColumnName));
                assertTrue(e instanceof TimelineException);
                assertEquals("OTSParameterInvalid", e.getMessage());
            }
        };

        Future<TimelineEntry> future = timelineQueue.batchStore(1, message, callback);
        try {
            TimelineEntry entry = future.get(5, TimeUnit.SECONDS);
            assertEquals("testFailedCallback", entry.getMessage().getString(invalidColumnName));

            assertTrue(future.isDone());
            assertFalse(future.isCancelled());
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testStoreAsyncFailedCallback() {
        final TimelineIdentifier originIdentifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "testStoreAsyncFailedCallback")
                .addField("long", 1000L)
                .build();

        TimelineQueue autoTimelineQueue = storeTableService.createTimelineQueue(originIdentifier);
        TimelineQueue manuTimelineQueue = syncTableService.createTimelineQueue(originIdentifier);

        final String invalidColumnName = LONG_INVALID_COLUME_NAME;
        final TimelineMessage message = new TimelineMessage()
                .setField(invalidColumnName, "testStoreAsyncFailedCallback");

        TimelineCallback callback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage timelineMessage, TimelineEntry timelineEntry) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncFailedCallback", timelineEntry.getMessage().getString("text"));
                assertEquals("testStoreAsyncFailedCallback", timelineMessage.getString("text"));
            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage timelineMessage, Exception e) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncFailedCallback", timelineMessage.getString(invalidColumnName));
                assertTrue(e instanceof TimelineException);
                assertEquals("OTSParameterInvalid", e.getMessage());
            }
        };

        Future<TimelineEntry> autoFuture = autoTimelineQueue.storeAsync(message, callback);
        Future<TimelineEntry> manuFuture = manuTimelineQueue.storeAsync(1, message, callback);
        try {
            TimelineEntry autoEntry = autoFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
        try {
            TimelineEntry manuEntry = manuFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testUpdateAsyncFailedCallback() {
        final TimelineIdentifier originIdentifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "testUpdateAsyncFailedCallback")
                .addField("long", 1000L)
                .build();

        TimelineQueue autoTimelineQueue = storeTableService.createTimelineQueue(originIdentifier);
        TimelineQueue manuTimelineQueue = syncTableService.createTimelineQueue(originIdentifier);

        final String invalidColumnName = LONG_INVALID_COLUME_NAME;
        final TimelineMessage autoMessage = new TimelineMessage()
                .setField("text", "testUpdateAsyncFailedCallback");
        final TimelineMessage manuMessage = new TimelineMessage()
                .setField(invalidColumnName, "testUpdateAsyncFailedCallback");

        TimelineCallback autoCallback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage timelineMessage, TimelineEntry timelineEntry) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testUpdateAsyncFailedCallback", timelineEntry.getMessage().getString("text"));
                assertEquals("testUpdateAsyncFailedCallback", timelineMessage.getString("text"));
            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage timelineMessage, Exception e) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testUpdateAsyncFailedCallback", timelineMessage.getString("text"));
                assertTrue(e instanceof TimelineException);
                assertEquals("OTSParameterInvalid", e.getMessage());
            }
        };

        TimelineCallback manuCallback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier identifier, TimelineMessage timelineMessage, TimelineEntry timelineEntry) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncFailedCallback", timelineEntry.getMessage().getString(invalidColumnName));
                assertEquals("testStoreAsyncFailedCallback", timelineMessage.getString(invalidColumnName));
            }

            @Override
            public void onFailed(TimelineIdentifier identifier, TimelineMessage timelineMessage, Exception e) {
                assertEquals(originIdentifier, identifier);
                assertEquals("testStoreAsyncFailedCallback", timelineMessage.getString(invalidColumnName));
                assertTrue(e instanceof TimelineException);
                assertEquals("OTSParameterInvalid", e.getMessage());
            }
        };


        Future<TimelineEntry> autoFuture = autoTimelineQueue.updateAsync(1, autoMessage, autoCallback);
        try {
            TimelineEntry autoEntry = autoFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSConditionCheckFail", e.getMessage());
        }

        Future<TimelineEntry> manuFuture = manuTimelineQueue.updateAsync(1, manuMessage, manuCallback);
        try {
            TimelineEntry manuEntry = manuFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testDeleteTimeline() {
        String[] groupMember = new String[]{"user_delete"};

        TimelineQueue groupTimelineQueue = syncTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "group_delete")
                        .addField("long", 1000L)
                        .build()
        );

        TimelineMessage tm = new TimelineMessage()
                .setField("text", "hello tablestore")
                .setField("receivers", groupMember)
                .setField("timestamp", 1)
                .setField("boolean", true);
        groupTimelineQueue.store(1111L, tm);

        /**
         * Delete stored message;
         * */
        groupTimelineQueue.delete(1111L);
        TimelineEntry deletedGroupEntry = groupTimelineQueue.get(1111L);
        assertNull(deletedGroupEntry);

        /**
         * Delete timeline which is not exist;
         * */
        try {
            groupTimelineQueue.delete(1111L);
            deletedGroupEntry = groupTimelineQueue.get(1111L);
            assertNull(deletedGroupEntry);
        } catch (Exception e) {
            fail();
        }

        TimelineQueue autoTimelineQueue = storeTableService.createTimelineQueue(
                new TimelineIdentifier.Builder()
                        .addField("timelineId", "group_auto_sequ_delete")
                        .addField("long", 1000L)
                        .build());

        TimelineMessage message = new TimelineMessage()
                .setField("text", "deleteNotExistAutoSequenceId");
        autoTimelineQueue.store(message);
        long sequenceId = autoTimelineQueue.getLatestSequenceId();

        autoTimelineQueue.delete(sequenceId);
        assertEquals(0, autoTimelineQueue.getLatestSequenceId());

        try {
            autoTimelineQueue.delete(sequenceId);
        } catch (Exception e) {
            fail();
        }
    }
}
