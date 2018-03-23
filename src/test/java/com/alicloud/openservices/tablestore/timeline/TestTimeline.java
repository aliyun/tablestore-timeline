package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.timeline.common.TimelineCallback;
import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.common.TimelineExceptionType;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.IStore;
import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class TestTimeline {
    class FakeStore implements IStore {
        IMessage message = null;
        String timelineID = null;
        Long sequenceID = null;
        ScanParameter parameter = null;
        TimelineCallback<IMessage> writeCallback = null;
        TimelineCallback<Long> readCallback = null;

        FakeStore() {}

        @Override
        public TimelineEntry write(String timelineID, IMessage message) {
            this.message = message;
            this.timelineID = timelineID;
            return null;
        }

        @Override
        public void batch(String timelineID, IMessage message) {
            this.timelineID = timelineID;
            this.message = message;
        }

        @Override
        public Future<TimelineEntry> writeAsync(String timelineID, IMessage message, TimelineCallback<IMessage> callback) {
            this.timelineID = timelineID;
            this.message = message;
            this.writeCallback = callback;
            return null;
        }

        @Override
        public TimelineEntry update(String timelineID, Long sequenceID, IMessage message) {
            this.message = message;
            this.timelineID = timelineID;
            this.sequenceID = sequenceID;
            return null;
        }

        @Override
        public Future<TimelineEntry> updateAsync(String timelineID, Long sequenceID, IMessage message, TimelineCallback<IMessage> callback) {
            this.timelineID = timelineID;
            this.sequenceID = sequenceID;
            this.message = message;
            this.writeCallback = callback;
            return null;
        }

        @Override
        public TimelineEntry read(String timelineID, Long sequenceID) {
            this.timelineID = timelineID;
            this.sequenceID = sequenceID;
            return null;
        }

        @Override
        public Future<TimelineEntry> readAsync(String timelineID, Long sequenceID, TimelineCallback<Long> callback) {
            this.timelineID = timelineID;
            this.sequenceID = sequenceID;
            this.readCallback = callback;
            return null;
        }

        @Override
        public Iterator<TimelineEntry> scan(String timelineID, ScanParameter parameter) {
            this.parameter = parameter;
            this.timelineID = timelineID;
            return null;
        }

        @Override
        public void create() {
        }

        @Override
        public void drop() {
        }

        @Override
        public boolean exist() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void testConstruct_InvalidParameter() {
        IStore store = new FakeStore();

        try {
            new Timeline(null, store);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }

        try {
            new Timeline("", store);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }

        try {
            new Timeline("1", null);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }
    }

    @Test
    public void testStore_InvalidParameter() {
        IStore store = new FakeStore();

        try {
            Timeline timeline = new Timeline("1", store);
            timeline.store(null);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }
    }

    @Test
    public void testStoreAsync_InvalidParameter() {
        IStore store = new FakeStore();

        try {
            Timeline timeline = new Timeline("1", store);
            timeline.storeAsync(null, null);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }
    }

    @Test
    public void testGet_InvalidParameter() {
        IStore store = new FakeStore();

        try {
            Timeline timeline = new Timeline("1", store);
            timeline.get(null);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }
    }

    @Test
    public void testGetAsync_InvalidParameter() {
        IStore store = new FakeStore();

        try {
            Timeline timeline = new Timeline("1", store);
            timeline.getAsync(null, null);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }
    }

    @Test
    public void testScan_InvalidParameter() {
        IStore store = new FakeStore();

        try {
            Timeline timeline = new Timeline("1", store);
            timeline.scan(null);
            fail();
        } catch (TimelineException ex) {
            assertEquals(TimelineExceptionType.INVALID_USE, ex.getType());
        }
    }

    @Test
    public void testWrite() {
        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            IMessage message = new StringMessage("111");
            timeline.store(message);

            assertEquals("1", store.timelineID);
            assertArrayEquals(message.serialize(), store.message.serialize());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testWriteAsync() {
        class FakeCallback implements TimelineCallback<IMessage> {

            @Override
            public void onCompleted(String timelineID, IMessage request, TimelineEntry timelineEntry) {
            }

            @Override
            public void onFailed(String timelineID, IMessage request, Exception ex) {
            }
        }

        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            IMessage message = new StringMessage("111");
            TimelineCallback<IMessage> callback = new FakeCallback();
            timeline.storeAsync(message, callback);

            assertEquals("1", store.timelineID);
            assertArrayEquals(message.serialize(), store.message.serialize());
            assertEquals(callback, store.writeCallback);
        } catch (TimelineException ex) {
            fail();
        }
    }

    @Test
    public void testUpdate() {
        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            IMessage message = new StringMessage("111");
            timeline.update(2L, message);

            assertEquals("1", store.timelineID);
            assertEquals(Long.valueOf(2), store.sequenceID);
            assertArrayEquals(message.serialize(), store.message.serialize());
        } catch (Exception ex) {
            fail();
        }
    }

    @Test
    public void testUpdateAsync() {
        class FakeCallback implements TimelineCallback<IMessage> {

            @Override
            public void onCompleted(String timelineID, IMessage request, TimelineEntry timelineEntry) {
            }

            @Override
            public void onFailed(String timelineID, IMessage request, Exception ex) {
            }
        }

        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            IMessage message = new StringMessage("111");
            TimelineCallback<IMessage> callback = new FakeCallback();
            timeline.updateAsync(2L, message, callback);

            assertEquals("1", store.timelineID);
            assertEquals(Long.valueOf(2), store.sequenceID);
            assertArrayEquals(message.serialize(), store.message.serialize());
            assertEquals(callback, store.writeCallback);
        } catch (TimelineException ex) {
            fail();
        }
    }

    @Test
    public void testGet() {
        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            Long sequenceID = 1001L;
            timeline.get(sequenceID);

            assertEquals(sequenceID, store.sequenceID);
        } catch (TimelineException ex) {
            fail();
        }
    }

    @Test
    public void testGetAsync() {
        class FakeCallback implements TimelineCallback<Long> {

            @Override
            public void onCompleted(String timelineID, Long request, TimelineEntry timelineEntry) {
            }

            @Override
            public void onFailed(String timelineID, Long request, Exception ex) {
            }
        }

        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            Long sequenceID = 1001L;
            TimelineCallback<Long> callback = new FakeCallback();
            timeline.getAsync(sequenceID, callback);

            assertEquals("1", store.timelineID);
            assertEquals(sequenceID, store.sequenceID);
            assertEquals(callback, store.readCallback);
        } catch (TimelineException ex) {
            fail();
        }
    }

    @Test
    public void testScan() {
        FakeStore store = new FakeStore();
        try {
            Timeline timeline = new Timeline("1", store);
            ScanParameterBuilder builder  = ScanParameterBuilder.scanForward();
            ScanParameter parameter = builder.maxCount(100).from(10).to(20).build();
            timeline.scan(parameter);

            assertEquals(parameter, store.parameter);
        } catch (TimelineException ex) {
            fail();
        }
    }
}
