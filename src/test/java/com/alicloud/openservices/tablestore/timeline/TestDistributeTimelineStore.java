package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.timeline.common.TimelineCallback;
import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineStore;
import com.alicloud.openservices.tablestore.timeline.store.IStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class TestDistributeTimelineStore {
    private final static String endpoint = "<your endpoint>";
    private final static String accessKeyID = "<your access key id>";
    private final static String accessKeySecret = "<your access key secret>";
    private final static String instanceName = "<your instance name>";
    private final static String testTablePrefix = "__timelinetest_ts_";
    private static SyncClient ots = null;
    private static DistributeTimelineConfig config = null;

    @Before
    public void setUp() throws Exception {
        ots = new SyncClient(endpoint, accessKeyID, accessKeySecret, instanceName);

        config = new DistributeTimelineConfig(endpoint, accessKeyID,
                accessKeySecret, instanceName, "base_timeline_test_table");
        config.setTtl(-1);
        config.setMessageInstance(new StringMessage());
    }

    @After
    public void after() throws Exception {
        List<String> tables = ots.listTable().getTableNames();
        for (String table: tables) {
            if (table.startsWith(testTablePrefix)) {
                ots.deleteTable(new DeleteTableRequest(table));
            }
        }
    }

    @Test
    public void testCreate() {
        config.setTableName(testTablePrefix + "testCreate");
        IStore store = new DistributeTimelineStore(config);

        assertTrue(!store.exist());

        try {
            store.drop();
        } catch (RuntimeException ex) {
            fail();
        }

        store.create();
        sleep(5);

        assertTrue(store.exist());

        try {
            store.create();
        } catch (RuntimeException ex) {
            fail();
        }

        store.drop();

        assertTrue(!store.exist());

        try {
            store.drop();
        } catch (RuntimeException ex) {
            fail();
        }
    }

    @Test
    public void testWrite_Exception() {
        config.setTableName(testTablePrefix + "testWrite_Exception");
        IStore store = new DistributeTimelineStore(config);
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);

        try {
            store.write(timelineID, message);
            fail();
        } catch (RuntimeException ex) {
            assertEquals("Store is not create, please create before write", ex.getMessage());
        }
    }

    @Test
    public void testWriteAsyncByFuture_Exception() {
        config.setTableName(testTablePrefix + "testWriteAsyncByFuture");
        IStore store = new DistributeTimelineStore(config);
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);

        Future<TimelineEntry> future = store.writeAsync(timelineID, message, null);
        try {
            future.get();
            fail();
        } catch (RuntimeException ex) {
            assertEquals("Store is not create, please create before write", ex.getMessage());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testWriteAsyncByCallback_Exception() {
        config.setTableName(testTablePrefix + "testWriteAsyncByCallback");
        IStore store = new DistributeTimelineStore(config);
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        final AtomicBoolean isDone = new AtomicBoolean(false);
        final AtomicReference<Exception> e = new AtomicReference<Exception>();

        store.writeAsync(timelineID, message, new TimelineCallback<IMessage>() {
            @Override
            public void onCompleted(String timelineID, IMessage request, TimelineEntry timelineEntry) {
                isDone.set(true);
            }

            @Override
            public void onFailed(String timelineID, IMessage request, Exception ex) {
                e.set(ex);
                isDone.set(true);
            }
        });
        while (!isDone.get()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException ex) {
               fail();
            }
        }

        assertTrue(e.get() instanceof RuntimeException);
        assertEquals("Store is not create, please create before write", e.get().getMessage());
    }

    @Test
    public void testWriteRead() {
        config.setTableName(testTablePrefix + "testWriteRead");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        TimelineEntry entry = store.write(timelineID, message);

        assertEquals(message, entry.getMessage());

        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        TimelineEntry entry2 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
    }

    @Test
    public void testWriteRead_Attribute() {
        config.setTableName(testTablePrefix + "testWriteRead_Attribute");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        message.addAttribute("name", "hangzhou");
        TimelineEntry entry = store.write(timelineID, message);

        assertEquals(message, entry.getMessage());

        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        TimelineEntry entry2 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
        assertEquals(1, entry2.getMessage().getAttributes().size());
        assertEquals("hangzhou", entry2.getMessage().getAttributes().get("name"));
    }

    @Test
    public void testWriteRead_Future() {
        config.setTableName(testTablePrefix + "testWriteRead_Future");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = "testWriteRead_Future";
        IMessage message = new StringMessage(content);
        Future<TimelineEntry> future = store.writeAsync(timelineID, message, null);
        assertTrue(future != null);

        Long sequenceID = 0L;
        try {
            TimelineEntry entry = future.get();
            assertEquals(message, entry.getMessage());
            sequenceID = entry.getSequenceID();
            assertTrue(sequenceID > 0);
        } catch (Exception ex) {
            fail();
        }

        Future<TimelineEntry> future2 = store.readAsync(timelineID, sequenceID, null);
        assertTrue(future2 != null);

        try {
            TimelineEntry entry2 = future2.get();
            assertEquals(sequenceID, entry2.getSequenceID());
            assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testWriteRead_Callback() {
        config.setTableName(testTablePrefix + "testWriteRead_Callback");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        final AtomicReference<TimelineEntry> entryRef = new AtomicReference<TimelineEntry>();
        final AtomicBoolean isFail = new AtomicBoolean(false);

        Future<TimelineEntry> future = store.writeAsync(timelineID, message, new TimelineCallback<IMessage>() {
            @Override
            public void onCompleted(String timelineID, IMessage request, TimelineEntry timelineEntry) {
                entryRef.set(timelineEntry);
            }

            @Override
            public void onFailed(String timelineID, IMessage request, Exception ex) {
                isFail.set(true);
            }
        });

        while (entryRef.get() == null && !isFail.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        TimelineEntry entry = entryRef.get();
        assertEquals(message, entry.getMessage());
        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        isFail.set(false);
        final AtomicReference<TimelineEntry> entryRef2 = new AtomicReference<TimelineEntry>();
        Future<TimelineEntry> future2 = store.readAsync(timelineID, sequenceID, new TimelineCallback<Long>() {
            @Override
            public void onCompleted(String timelineID, Long request, TimelineEntry timelineEntry) {
                entryRef2.set(timelineEntry);
            }

            @Override
            public void onFailed(String timelineID, Long request, Exception ex) {
                isFail.set(true);
            }
        });

        while (entryRef2.get() == null && !isFail.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        TimelineEntry entry2 = entryRef2.get();
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
    }

    @Test
    public void testScan_Exception() {
        config.setTableName(testTablePrefix + "testScan_Exception");
        IStore store = new DistributeTimelineStore(config);
        String timelineID = "00001";

        ScanParameter parameter = ScanParameterBuilder.scanForward().maxCount(100).from(0).to(100).build();

        try {
            store.scan(timelineID, parameter);
            fail();
        } catch (RuntimeException ex) {
            assertEquals("Store is not create, please create before scan", ex.getMessage());
        }
    }

    @Test
    public void testScanForward_NoData() {
        config.setTableName(testTablePrefix + "testScanForward_NoData");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        ScanParameter parameter = ScanParameterBuilder.scanForward().maxCount(100).from(0).to(100).build();

        try {
            Iterator<TimelineEntry> iterator = store.scan(timelineID, parameter);
            assertTrue(!iterator.hasNext());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testScanBackward_NoData() {
        config.setTableName(testTablePrefix + "testScanForward_NoData");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        ScanParameter parameter = ScanParameterBuilder.scanBackward().maxCount(100).from(110).to(100).build();

        try {
            Iterator<TimelineEntry> iterator = store.scan(timelineID, parameter);
            assertTrue(!iterator.hasNext());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testScanForward_HasData() {
        config.setTableName(testTablePrefix + "testScanForward_HasData");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content1 = String.valueOf(new Date().getTime());
        IMessage message1 = new StringMessage(content1);
        TimelineEntry entry1 = store.write(timelineID, message1);
        Long sequence1 = entry1.getSequenceID();

        String content2 = String.valueOf(new Date().getTime() + 1);
        IMessage message2 = new StringMessage(content2);
        TimelineEntry entry2 = store.write(timelineID, message2);
        Long sequence2 = entry2.getSequenceID();

        ScanParameter parameter = ScanParameterBuilder.scanForward().maxCount(100).from(0).to(Long.MAX_VALUE).build();

        try {
            Iterator<TimelineEntry> iterator = store.scan(timelineID, parameter);

            assertTrue(iterator.hasNext());
            TimelineEntry entry = iterator.next();
            assertEquals(sequence1, entry.getSequenceID());
            assertEquals(new String(message1.serialize()), new String(entry.getMessage().serialize()));

            assertTrue(iterator.hasNext());
            entry = iterator.next();
            assertEquals(sequence2, entry.getSequenceID());
            assertEquals(new String(message2.serialize()), new String(entry.getMessage().serialize()));

            assertTrue(!iterator.hasNext());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testScanForwardWithFilter() {
        config.setTableName(testTablePrefix + "testScanForwardWithFilter");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content1 = String.valueOf(new Date().getTime());
        IMessage message1 = new StringMessage(content1);
        message1.addAttribute("name", "hangzhou");
        TimelineEntry entry1 = store.write(timelineID, message1);
        Long sequence1 = entry1.getSequenceID();

        String content2 = String.valueOf(new Date().getTime() + 1);
        IMessage message2 = new StringMessage(content2);
        message2.addAttribute("name", "xi'an");
        store.write(timelineID, message2);

        Filter filter = new SingleColumnValueFilter("name", SingleColumnValueFilter.CompareOperator.EQUAL,
                ColumnValue.fromString("hangzhou"));
        ScanParameter parameter = ScanParameterBuilder.scanForward()
                .maxCount(100).from(0).to(Long.MAX_VALUE).filter(filter)
                .build();

        try {
            Iterator<TimelineEntry> iterator = store.scan(timelineID, parameter);

            assertTrue(iterator.hasNext());
            TimelineEntry entry = iterator.next();
            assertEquals(sequence1, entry.getSequenceID());
            assertEquals(new String(message1.serialize()), new String(entry.getMessage().serialize()));

            assertTrue(!iterator.hasNext());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testScanBackward_HasData() {
        config.setTableName(testTablePrefix + "testScanBackward_HasData");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content1 = String.valueOf(new Date().getTime());
        IMessage message1 = new StringMessage(content1);
        TimelineEntry entry1 = store.write(timelineID, message1);
        Long sequence1 = entry1.getSequenceID();

        String content2 = String.valueOf(new Date().getTime() + 1);
        IMessage message2 = new StringMessage(content2);
        TimelineEntry entry2 = store.write(timelineID, message2);
        Long sequence2 = entry2.getSequenceID();

        ScanParameter parameter = ScanParameterBuilder.scanBackward().maxCount(100).from(Long.MAX_VALUE).to(0).build();

        try {
            Iterator<TimelineEntry> iterator = store.scan(timelineID, parameter);

            assertTrue(iterator.hasNext());
            TimelineEntry entry = iterator.next();
            assertEquals(sequence2, entry.getSequenceID());
            assertEquals(new String(message2.serialize()), new String(entry.getMessage().serialize()));

            assertTrue(iterator.hasNext());
            entry = iterator.next();
            assertEquals(sequence1, entry.getSequenceID());
            assertEquals(new String(message1.serialize()), new String(entry.getMessage().serialize()));

            assertTrue(!iterator.hasNext());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testBatchScan() {
        config.setTableName(testTablePrefix + "testBatchScan");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        store.batch(timelineID, message);

        ScanParameter parameter = ScanParameterBuilder.scanBackward().maxCount(100).from(Long.MAX_VALUE).to(0).build();
        Iterator<TimelineEntry> iterator = store.scan(timelineID, parameter);

        assertTrue(!iterator.hasNext());

        sleep(config.getWriterConfig().getFlushInterval());

        iterator = store.scan(timelineID, parameter);
        assertTrue(iterator.hasNext());
        TimelineEntry entry = iterator.next();
        assertTrue(entry.getSequenceID() > 0);
        assertEquals(message.getMessageID(), entry.getMessage().getMessageID());
        assertEquals(new String(message.serialize()), new String(entry.getMessage().serialize()));
    }

    @Test
    public void testWriteUpdateRead_Attribute() {
        config.setTableName(testTablePrefix + "testWriteUpdateRead_Attribute");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        /**
         * Write
         */
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        message.addAttribute("name", "hangzhou");
        TimelineEntry entry = store.write(timelineID, message);

        assertEquals(message, entry.getMessage());

        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        /**
         * Read
         */
        TimelineEntry entry2 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
        assertEquals(1, entry2.getMessage().getAttributes().size());
        assertEquals("hangzhou", entry2.getMessage().getAttributes().get("name"));

        /**
         * Update
         */
        message.updateAttribute("name", "chengdu");
        entry = store.update(timelineID, entry2.getSequenceID(), message);

        assertEquals(message, entry.getMessage());

        sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        /**
         * Read
         */
        TimelineEntry entry3 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry3.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry3.getMessage().serialize()));
        assertEquals(1, entry3.getMessage().getAttributes().size());
        assertEquals("chengdu", entry3.getMessage().getAttributes().get("name"));
    }

    @Test
    public void testWriteUpdateRead_Content() {
        config.setTableName(testTablePrefix + "testWriteUpdateRead_Content");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        /**
         * Write
         */
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        TimelineEntry entry = store.write(timelineID, message);

        assertEquals(message, entry.getMessage());

        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        /**
         * Read
         */
        TimelineEntry entry2 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
        assertEquals(0, entry2.getMessage().getAttributes().size());

        /**
         * Update
         */
        IMessage message2 = new StringMessage(content + "1");
        entry = store.update(timelineID, entry2.getSequenceID(), message2);

        assertEquals(message2, entry.getMessage());

        sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        /**
         * Read
         */
        TimelineEntry entry3 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry3.getSequenceID());
        assertEquals(new String(message2.serialize()), new String(entry3.getMessage().serialize()));
        assertEquals(0, entry2.getMessage().getAttributes().size());
    }

    @Test
    public void testWriteUpdateRead_Content2() {
        DistributeTimelineConfig localConfig = config;
        localConfig.setTableName(testTablePrefix + "testWriteUpdateRead_Content2");
        localConfig.setColumnMaxLength(2);
        IStore store = new DistributeTimelineStore(localConfig);
        store.create();
        sleep(5);

        /**
         * Write
         */
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content + content + "124");
        TimelineEntry entry = store.write(timelineID, message);

        assertEquals(message, entry.getMessage());

        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        /**
         * Read
         */
        TimelineEntry entry2 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
        assertEquals(0, entry2.getMessage().getAttributes().size());

        /**
         * Update
         */
        IMessage message2 = new StringMessage("123");
        entry = store.update(timelineID, sequenceID, message2);

        assertEquals(message2, entry.getMessage());

        sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        /**
         * Read
         */
        TimelineEntry entry3 = store.read(timelineID, sequenceID);
        assertEquals(sequenceID, entry3.getSequenceID());
        assertEquals(new String(message2.serialize()), new String(entry3.getMessage().serialize()));
        assertEquals(0, entry3.getMessage().getAttributes().size());
    }

    @Test
    public void testUpdate_Exception() {
        config.setTableName(testTablePrefix + "testUpdate_Exception");
        IStore store = new DistributeTimelineStore(config);

        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);

        try {
            store.update(timelineID, 10000L, message);
            fail();
        } catch (TimelineException ex) {
            assertEquals("Parameter is invalid, reason:Condition check failed.", ex.getMessage());
        }
    }

    @Test
    public void testUpdate() {
        config.setTableName(testTablePrefix + "testUpdate");
        IStore store = new DistributeTimelineStore(config);

        store.create();
        sleep(5);

        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);

        TimelineEntry entry = store.write(timelineID, message);

        try {
            store.update(timelineID, entry.getSequenceID(), message);
        } catch (TimelineException ex) {
            fail();
        }
    }

    @Test
    public void testWriteUpdateRead_Future() {
        config.setTableName(testTablePrefix + "testWriteUpdateRead_Future");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        /**
         * Write
         */
        String timelineID = "00001";
        String content = "testWriteRead_Future";
        IMessage message = new StringMessage(content);
        message.addAttribute("name", "xian");
        Future<TimelineEntry> future = store.writeAsync(timelineID, message, null);
        assertTrue(future != null);

        Long sequenceID = 0L;
        try {
            TimelineEntry entry = future.get();
            assertEquals(message, entry.getMessage());
            sequenceID = entry.getSequenceID();
            assertTrue(sequenceID > 0);
        } catch (Exception ex) {
            fail();
        }

        /**
         * Read
         */
        Future<TimelineEntry> future2 = store.readAsync(timelineID, sequenceID, null);
        assertTrue(future2 != null);

        try {
            TimelineEntry entry2 = future2.get();
            assertEquals(sequenceID, entry2.getSequenceID());
            assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
            assertEquals("xian", message.getAttributes().get("name"));
        } catch (Exception ex) {
            fail();
        }

        /**
         * Update
         */
        future = store.updateAsync(timelineID, sequenceID, message, null);
        assertTrue(future != null);

        try {
            TimelineEntry entry = future.get();
            message.updateAttribute("name", "shanghai");
            assertEquals(message, entry.getMessage());
            sequenceID = entry.getSequenceID();
            assertTrue(sequenceID > 0);
        } catch (Exception ex) {
            fail();
        }

        /**
         * Read
         */
        future2 = store.readAsync(timelineID, sequenceID, null);
        assertTrue(future2 != null);

        try {
            TimelineEntry entry2 = future2.get();
            assertEquals(sequenceID, entry2.getSequenceID());
            assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
            assertEquals("shanghai", message.getAttributes().get("name"));
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testWriteUpdateRead_Callback() {
        config.setTableName(testTablePrefix + "testWriteUpdateRead_Callback");
        IStore store = new DistributeTimelineStore(config);
        store.create();
        sleep(5);

        /**
         * Write
         */
        String timelineID = "00001";
        String content = String.valueOf(new Date().getTime());
        IMessage message = new StringMessage(content);
        message.addAttribute("age", "18");
        final AtomicReference<TimelineEntry> entryRef = new AtomicReference<TimelineEntry>();
        final AtomicBoolean isFail = new AtomicBoolean(false);

        Future<TimelineEntry> future = store.writeAsync(timelineID, message, new TimelineCallback<IMessage>() {
            @Override
            public void onCompleted(String timelineID, IMessage request, TimelineEntry timelineEntry) {
                entryRef.set(timelineEntry);
            }

            @Override
            public void onFailed(String timelineID, IMessage request, Exception ex) {
                isFail.set(true);
            }
        });

        while (entryRef.get() == null && !isFail.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        /**
         * Read
         */
        TimelineEntry entry = entryRef.get();
        assertEquals(message, entry.getMessage());
        Long sequenceID = entry.getSequenceID();
        assertTrue(sequenceID> 0);

        final AtomicReference<TimelineEntry> entryRef2 = new AtomicReference<TimelineEntry>();
        final AtomicBoolean isFail2 = new AtomicBoolean(false);
        Future<TimelineEntry> future2 = store.readAsync(timelineID, sequenceID, new TimelineCallback<Long>() {
            @Override
            public void onCompleted(String timelineID, Long request, TimelineEntry timelineEntry) {
                entryRef2.set(timelineEntry);
            }

            @Override
            public void onFailed(String timelineID, Long request, Exception ex) {
                isFail2.set(true);
            }
        });

        while (entryRef2.get() == null && !isFail2.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        TimelineEntry entry2 = entryRef2.get();
        assertEquals(sequenceID, entry2.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry2.getMessage().serialize()));
        assertEquals("18", message.getAttributes().get("age"));

        /**
         * Update
         */
        message.updateAttribute("age", "20");
        final AtomicReference<TimelineEntry> entryRef3 = new AtomicReference<TimelineEntry>();
        final AtomicBoolean isFail3 = new AtomicBoolean(false);

        future = store.writeAsync(timelineID, message, new TimelineCallback<IMessage>() {
            @Override
            public void onCompleted(String timelineID, IMessage request, TimelineEntry timelineEntry) {
                entryRef3.set(timelineEntry);
            }

            @Override
            public void onFailed(String timelineID, IMessage request, Exception ex) {
                isFail3.set(true);
            }
        });

        while (entryRef3.get() == null && !isFail3.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        /**
         * Read
         */
        final AtomicReference<TimelineEntry> entryRef4 = new AtomicReference<TimelineEntry>();
        final AtomicBoolean isFail4 = new AtomicBoolean(false);

        Future<TimelineEntry> future4 = store.readAsync(timelineID, sequenceID, new TimelineCallback<Long>() {
            @Override
            public void onCompleted(String timelineID, Long request, TimelineEntry timelineEntry) {
                entryRef4.set(timelineEntry);
            }

            @Override
            public void onFailed(String timelineID, Long request, Exception ex) {
                isFail4.set(true);
            }
        });

        while (entryRef4.get() == null && !isFail4.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {
                fail();
            }
        }

        TimelineEntry entry4 = entryRef4.get();
        assertEquals(sequenceID, entry4.getSequenceID());
        assertEquals(new String(message.serialize()), new String(entry4.getMessage().serialize()));
        assertEquals("20", message.getAttributes().get("age"));
    }

    private void sleep(int seconds) {
        try {
            Thread.sleep(seconds);
        } catch (InterruptedException e) {
            fail();
        }
    }

}
