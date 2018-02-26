package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineStore;
import com.alicloud.openservices.tablestore.timeline.store.IStore;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;

public class TestTimelineWritePerf {
    private final static String endpoint = "<your endpoint>";
    private final static String accessKeyID = "<your access key id>";
    private final static String accessKeySecret = "<your access key secret>";
    private final static String instanceName = "<your instance name>";
    private final static String tableName = "__timelinetest_perf_ts_";

    private final static DistributeTimelineConfig config = new DistributeTimelineConfig(endpoint, accessKeyID,
            accessKeySecret, instanceName, tableName);
    private final static SyncClient ots = new SyncClient(endpoint, accessKeyID, accessKeySecret, instanceName);

    private static Long threadCount = 1L;
    private static Long timelineCount = 1L;
    private static Long messageCountPerTimeline = 1L;
    private static Long messageLength = 0L;
    private static AtomicLong totalCount = new AtomicLong();


    @Before
    public void setUp() throws Exception {
    }

    @After
    public void after() throws Exception {
        List<String> tables = ots.listTable().getTableNames();
        for (String table: tables) {
            if (table.equals(tableName)) {
                ots.deleteTable(new DeleteTableRequest(table));
            }
        }
    }

    @Test
    public void testWrite() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        List<Thread> threads = new ArrayList<Thread>();
        final long timelineCountPerThread = timelineCount / threadCount;

        final long timelineCountOfLastThread = timelineCount % threadCount == 0 ?
                timelineCountPerThread : (timelineCount % threadCount);

        final long messageCount = messageCountPerTimeline;

        final IStore store = new DistributeTimelineStore(config);
        if (!store.exist()) {
            store.create();
        }


        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            final long timelineCount = (threadId == threadCount - 1 )? timelineCountOfLastThread : timelineCountPerThread;
            final long startTimelineID = timelineCountPerThread * threadId;

            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    String content = "";
                    for (int k = 0; k < messageLength; k++) {
                        content = content + "x";
                    }
                    IMessage message = new StringMessage(content);

                    for (int m = 0; m < messageCount; m++) {
                        for (int j = 0; j < timelineCount; j++) {
                            String timelineID = getMd5(String.valueOf(startTimelineID + j));
                            Timeline timeline = new Timeline(timelineID, store);
                            timeline.store(message);
                            totalCount.incrementAndGet();
                        }
                    }
                }
            });
            threads.add(th);
        }

        long st = System.currentTimeMillis();

        for (Thread th : threads) {
            th.start();
        }

        for (Thread th : threads) {
            th.join();
        }

        store.close();

        long et = System.currentTimeMillis();
        Long totalTime = et - st;

        System.out.println("TotalTime: " + totalTime);
        double qps = 1.0 * (totalCount.get()) / (totalTime / 1000.0);
        System.out.println("QPS: " + qps);
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static String getMd5(String source) {
        try {
            byte[] bytesOfMessage = source.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] thedigest = Base64.encodeBase64(md.digest(bytesOfMessage));
            return new String(thedigest).substring(0, 4);
        } catch (Exception ex) {
            fail();
            return "";
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Arguments not enough. timelineCount threadCount messageCountPerTimeline messageLength ioThreadCount");
            System.exit(-1);
        }

        timelineCount = Long.parseLong(args[0]);
        threadCount = Long.parseLong(args[1]);
        messageCountPerTimeline = Long.parseLong(args[2]);
        messageLength = Long.parseLong(args[3]);
        int ioThreadCount = Integer.parseInt(args[4]);

        System.out.println("TimelineCount: " + timelineCount);
        System.out.println("ThreadCount: " + threadCount);
        System.out.println("MessageCount: " + messageCountPerTimeline);
        System.out.println("messageLength: " + messageLength);
        System.out.println("ioThreadCount: " + ioThreadCount);

        TestTimelineWritePerf timelinePerf = new TestTimelineWritePerf();
        timelinePerf.setUp();
        timelinePerf.testWrite();
        timelinePerf.after();
    }
}
