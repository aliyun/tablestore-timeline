package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.DeleteTableRequest;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineStore;
import com.alicloud.openservices.tablestore.timeline.store.IStore;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.fail;

public class TestTimelineScanPerf {
    private final static String endpoint = "<your endpoint>";
    private final static String accessKeyID = "<your access key id>";
    private final static String accessKeySecret = "<your access key secret>";
    private final static String instanceName = "<your instance name>";

    private static DistributeTimelineConfig config;
    private static SyncClient ots = null;
    private final static String testTablePrefix = "__timelinetest_perf_ts_";

    private static Long threadCount = 1L;
    private static Long timelineCount = 1L;
    private static Long messageCountPerScan = 1L;
    private static Long totalTime = 0L;
    private static Long scanCount = 0L;
    private static AtomicLong totalCount = new AtomicLong();


    @Before
    public void setUp() throws Exception {
        ots = new SyncClient(endpoint, accessKeyID, accessKeySecret, instanceName);
        config = new DistributeTimelineConfig(endpoint, accessKeyID, accessKeySecret,
                instanceName, "base_timeline_test_table");
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
    public void testScan() throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        List<Thread> threads = new ArrayList<Thread>();
        final long timelineCountPerThread = timelineCount / threadCount;

        final long timelineCountOfLastThread = timelineCount % threadCount == 0 ?
                timelineCountPerThread : (timelineCount % threadCount);

        final IStore store = new DistributeTimelineStore(config);
        if (!store.exist()) {
            store.create();
        }

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            final ScanParameter parameter = ScanParameterBuilder.scanForward().from(0).to(Long.MAX_VALUE).maxCount(20).build();

            Thread th = new Thread(new Runnable() {

                @Override
                public void run() {
                    long timelineCount = timelineCountPerThread;
                    if (threadId == threadCount - 1) {
                        timelineCount = timelineCountOfLastThread;
                    }

                    long startTimelineID = timelineCountPerThread * threadId;
                    Map<String, Timeline> timelineMap = new HashMap<String, Timeline>();
                    for (int m = 0; m < scanCount; m++) {
                        for (int j = 0; j < timelineCount; j++) {
                            String timelineID = getMd5(String.valueOf(startTimelineID + j));
                            Timeline timeline = null;
                            if (timelineMap.containsKey(timelineID)) {
                                timeline = timelineMap.get(timelineID);
                            } else {
                                timeline = new Timeline(timelineID, store);
                                timelineMap.put(timelineID, timeline);
                            }

                            Iterator<TimelineEntry> iter = timeline.scan(parameter);

                            int count = 0;
                            while (iter.hasNext()) {
                                count +=1;
                                iter.next();
                                if (count == 20) {
                                    break;
                                }
                            }

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

        long et = System.currentTimeMillis();
        totalTime = et - st;

        store.close();

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
        if (args.length < 4) {
            System.out.println("Arguments not enough. timelineCount threadCount messageCountPerScan scanCount");
            System.exit(-1);
        }

        timelineCount = Long.parseLong(args[0]);
        threadCount = Long.parseLong(args[1]);
        messageCountPerScan = Long.parseLong(args[2]);
        scanCount = Long.parseLong(args[3]);

        System.out.println("TimelineCount: " + timelineCount);
        System.out.println("ThreadCount: " + threadCount);
        System.out.println("messageCountPerScan: " + messageCountPerScan);
        System.out.println("scanCount: " + scanCount);

        TestTimelineScanPerf timelinePerf = new TestTimelineScanPerf();
        timelinePerf.setUp();
        timelinePerf.testScan();
        timelinePerf.after();
    }
}
