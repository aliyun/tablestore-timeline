package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.timeline.ScanParameterBuilder;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline2.common.Conf;
import com.alicloud.openservices.tablestore.timeline2.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.ScanParameter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestTimelineV1 {
    private static String endpoint;
    private static String accessKeyId;
    private static String accessKeySecret;
    private static String instanceName;
    private static String storeTable = "storeTable";
    private static String syncTable = "syncTable";

    private static String firstPKName = "timelineId";
    private static String sequenceColumnName = "sequence_id";

    private static DistributeTimelineStore store = null;
    private static DistributeTimelineStore sync = null;

    private static TimelineStore storeService = null;
    private static TimelineStore syncService = null;
    private static String longString;

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            Conf conf = Conf.newInstance(System.getProperty("user.home") + "/timelineConf.json");
            endpoint = conf.getEndpoint();
            accessKeyId = conf.getAccessId();
            accessKeySecret = conf.getAccessKey();
            instanceName = conf.getInstanceName();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        initTimelineV1();

        initTimelineV2();

        longString = generateLongContent();
    }

    @AfterClass
    public static void after() throws Exception {
        storeService.dropAllTables();
        syncService.dropAllTables();
    }

    @Test
    public void testStoreMessageByV1() {
        List<String> groupMembers = Arrays.asList("user_A_v1");
        List<String> contents = Arrays.asList("message_0", "message_1", "message_2");

        sendGroupMessageV1("group_v1", new StringMessage(longString + contents.get(0)), groupMembers);
        sendGroupMessageV1("group_v1", new StringMessage(longString + contents.get(1)), groupMembers);
        sendGroupMessageV1("group_v1", new StringMessage(longString + contents.get(2)), groupMembers);

        /**
         * Scan store timeline by v2
         */
        TimelineIdentifier group = new TimelineIdentifier.Builder()
                .addField(firstPKName, "group_v1")
                .build();

        TimelineQueue storeTimelineQueue = storeService.createTimelineQueue(group);

        Iterator<TimelineEntry> storeIterator = storeTimelineQueue.scan(new ScanParameter().scanBackward(Long.MAX_VALUE));
        int counter = 2;
        while (storeIterator.hasNext()) {
            TimelineEntry entry = storeIterator.next();
            TimelineMessageForV1 message = new TimelineMessageForV1(entry.getMessage());
            assertEquals(longString + "message_" + counter--, message.getContent());

            storeTimelineQueue.delete(entry.getSequenceID());
            TimelineEntry deteledEntry = storeTimelineQueue.get(entry.getSequenceID());
            assertNull(deteledEntry);
        }
        assertEquals(-1, counter);

        /**
         * Scan sync timeline by v2
         */
        TimelineIdentifier user = new TimelineIdentifier.Builder()
                .addField(firstPKName, "user_A_v1")
                .build();
        TimelineQueue syncTimelineQueue = syncService.createTimelineQueue(user);

        Iterator<TimelineEntry> syncIterator = syncTimelineQueue.scan(new ScanParameter().scanBackward(Long.MAX_VALUE));
        counter = 2;
        while (syncIterator.hasNext()) {
            TimelineEntry entry = syncIterator.next();
            TimelineMessageForV1 message = new TimelineMessageForV1(entry.getMessage());
            assertEquals(longString + "message_" + counter--, message.getContent());

            syncTimelineQueue.delete(entry.getSequenceID());
            TimelineEntry deteledEntry = syncTimelineQueue.get(entry.getSequenceID());
            assertNull(deteledEntry);
        }
        assertEquals(-1, counter);
    }

    @Test
    public void testStoreMessageByV2() {
        /**
         * store timeline by v2
         */
        List<String> groupMembers = Arrays.asList("user_B_v2");
        List<String> contents = Arrays.asList("message_0", "message_1", "message_2");

        sendMessageV1ByV2("group_v2", new TimelineMessageForV1(longString + contents.get(0)), groupMembers);
        sendMessageV1ByV2("group_v2", new TimelineMessageForV1(longString + contents.get(1)), groupMembers);
        sendMessageV1ByV2("group_v2", new TimelineMessageForV1(longString + contents.get(2)), groupMembers);

        /**
         * Scan store timeline by v1
         */
        com.alicloud.openservices.tablestore.timeline.Timeline groupV1 = new com.alicloud.openservices.tablestore.timeline.Timeline("group_v2", store);
        com.alicloud.openservices.tablestore.timeline.ScanParameter scanParameter = ScanParameterBuilder
                .scanBackward()
                .from(Long.MAX_VALUE)
                .to(0)
                .maxCount(100)
                .build();
        Iterator<com.alicloud.openservices.tablestore.timeline.TimelineEntry> entries = groupV1.scan(scanParameter);

        TimelineIdentifier groupV2 = new TimelineIdentifier.Builder()
                .addField(firstPKName, "group_v2")
                .build();
        TimelineQueue storeTimelineQueue = storeService.createTimelineQueue(groupV2);

        int counter = 2;
        while (entries.hasNext()) {
            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry = entries.next();
            assertEquals(longString + "message_" + counter, new String(entry.getMessage().serialize()));

            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry1 = groupV1.get(entry.getSequenceID());
            assertEquals(longString + "message_" + counter, new String(entry1.getMessage().serialize()));

            TimelineEntry entryV2 = storeTimelineQueue.get(entry.getSequenceID());
            assertEquals(longString + "message_" + counter, new TimelineMessageForV1(entryV2.getMessage()).getContent());

            storeTimelineQueue.delete(entry.getSequenceID());

            counter--;
        }
        assertEquals(-1, counter);

        /**
         * Scan sync timeline by v1
         */
        com.alicloud.openservices.tablestore.timeline.Timeline userV1 = new com.alicloud.openservices.tablestore.timeline.Timeline("user_B_v2", sync);
        com.alicloud.openservices.tablestore.timeline.ScanParameter userScanParameter = ScanParameterBuilder
                .scanBackward()
                .from(Long.MAX_VALUE)
                .to(0)
                .maxCount(100)
                .build();
        Iterator<com.alicloud.openservices.tablestore.timeline.TimelineEntry> userEntries = userV1.scan(userScanParameter);
        TimelineIdentifier userB = new TimelineIdentifier.Builder()
                .addField(firstPKName, "user_B_v2")
                .build();
        TimelineQueue syncTimelineQueue = syncService.createTimelineQueue(userB);

        counter = 2;
        while (userEntries.hasNext()) {
            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry = userEntries.next();
            assertEquals(longString + "message_" + counter, new String(entry.getMessage().serialize()));

            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry1 = userV1.get(entry.getSequenceID());
            assertEquals(longString + "message_" + counter, new String(entry1.getMessage().serialize()));

            TimelineEntry entryV2 = syncTimelineQueue.get(entry.getSequenceID());
            assertEquals(longString + "message_" + counter, new TimelineMessageForV1(entryV2.getMessage()).getContent());

            syncTimelineQueue.delete(entry.getSequenceID());

            counter--;
        }
        assertEquals(-1, counter);
    }

    @Test
    public void testUpdateMessageByV2() {
        /**
         * store timeline by v2
         */
        List<String> groupMembers = Arrays.asList("user_update");

        sendGroupMessageV1("group_update", new StringMessage("message_v1"), groupMembers);


        /**
         * update store timeline by v1 and check by v2
         */
        com.alicloud.openservices.tablestore.timeline.Timeline groupV1 = new com.alicloud.openservices.tablestore.timeline.Timeline("group_update", store);
        com.alicloud.openservices.tablestore.timeline.ScanParameter scanParameter = ScanParameterBuilder
                .scanBackward()
                .from(Long.MAX_VALUE)
                .to(0)
                .maxCount(100)
                .build();
        Iterator<com.alicloud.openservices.tablestore.timeline.TimelineEntry> entries = groupV1.scan(scanParameter);

        TimelineIdentifier groupV2 = new TimelineIdentifier.Builder()
                .addField(firstPKName, "group_update")
                .build();
        TimelineQueue storeTimelineQueue = storeService.createTimelineQueue(groupV2);

        if (entries.hasNext()) {
            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry = entries.next();
            assertEquals("message_v1" , new String(entry.getMessage().serialize()));

            IMessage message = entry.getMessage();
            message.setMessageID("new message id v1");
            message.addAttribute("attr","new message id v1");

            groupV1.update(entry.getSequenceID(), message);

            TimelineEntry newTimelineEntry = storeTimelineQueue.get(entry.getSequenceID());
            TimelineMessageForV1 newMessage = new TimelineMessageForV1(newTimelineEntry.getMessage());
            assertEquals("new message id v1", newMessage.getAttribute("attr"));
        }


        /**
         * update store timeline by v2 and check by v2 & v1
         */
        com.alicloud.openservices.tablestore.timeline.Timeline userV1 = new com.alicloud.openservices.tablestore.timeline.Timeline("user_update", sync);

        TimelineIdentifier userV2 = new TimelineIdentifier.Builder()
                .addField(firstPKName, "user_update")
                .build();
        TimelineQueue syncTimelineQueue = syncService.createTimelineQueue(userV2);

        TimelineEntry timelineBefore = syncTimelineQueue.getLatestTimelineEntry();
        TimelineMessageForV1 userMessage = new TimelineMessageForV1(timelineBefore.getMessage());
        userMessage.setContent("new content");
        userMessage.setMessageId("new message id");
        userMessage.addAttribute("attr", "new field");

        syncTimelineQueue.update(timelineBefore.getSequenceID(), userMessage.getTimelineMessage());
        TimelineEntry timelineAfter = syncTimelineQueue.get(timelineBefore.getSequenceID());
        userMessage = new TimelineMessageForV1(timelineAfter.getMessage());

        assertEquals("new content", userMessage.getContent());
        assertEquals("new message id", userMessage.getMessageID());
        assertEquals("new field", userMessage.getAttribute("attr"));

        com.alicloud.openservices.tablestore.timeline.TimelineEntry timelineEntryV1 = userV1.get(timelineBefore.getSequenceID());
        assertEquals("new content", new String(timelineEntryV1.getMessage().serialize()));
        assertEquals("new message id", timelineEntryV1.getMessage().getMessageID());
        assertEquals("new field", timelineEntryV1.getMessage().getAttributes().get("attr"));
    }

    private void sendGroupMessageV1(String groupName, IMessage message, List<String> groupMembers) {
        com.alicloud.openservices.tablestore.timeline.Timeline sender = new com.alicloud.openservices.tablestore.timeline.Timeline(groupName, store);
        sender.store(message);

        for (String user : groupMembers) {
            com.alicloud.openservices.tablestore.timeline.Timeline receiver = new com.alicloud.openservices.tablestore.timeline.Timeline(user, sync);
            receiver.store(message);
        }
    }

    private void sendMessageV1ByV2(String groupName, TimelineMessageForV1 messageV1, List<String> groupMembers) {
        TimelineIdentifier groupTable = new TimelineIdentifier.Builder()
                .addField(firstPKName, groupName)
                .build();

        TimelineQueue storeTimelineQueue = storeService.createTimelineQueue(groupTable);
        TimelineMessage timelineMessage = messageV1.getTimelineMessage();
        storeTimelineQueue.store(timelineMessage);

        for (String user : groupMembers) {
            TimelineIdentifier userTable = new TimelineIdentifier.Builder()
                    .addField(firstPKName, user)
                    .build();
            TimelineQueue syncTimelineQueue = syncService.createTimelineQueue(userTable);

            syncTimelineQueue.store(timelineMessage);
        }


    }


    private static void initTimelineV1() {
        /**
         * init timeline services by v1, and create tables;
         */
        DistributeTimelineConfig storeConfig = new DistributeTimelineConfig(endpoint, accessKeyId, accessKeySecret, instanceName, storeTable);
        storeConfig.setFirstPKName(firstPKName);
        storeConfig.setMessageInstance(new StringMessage());
        storeConfig.setTtl(-1); // one Year
        storeConfig.setLimit(100);

        DistributeTimelineConfig syncConfig = new DistributeTimelineConfig(endpoint, accessKeyId, accessKeySecret, instanceName, syncTable);
        syncConfig.setFirstPKName(firstPKName);
        syncConfig.setMessageInstance(new StringMessage());
        syncConfig.setTtl(30 * 24 * 3600); // one Month
        syncConfig.setLimit(100);

        store = new DistributeTimelineStore(storeConfig);
        sync = new DistributeTimelineStore(syncConfig);

        store.create();
        sync.create();
    }

    private static void initTimelineV2() {
        /**
         * init timeline services by v2
         */
        TimelineStoreFactory serviceFactory = new TimelineStoreFactoryImpl(
                new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)
        );

        //the pkName of the first PrimaryKey should be the same as vi timelineId
        TimelineIdentifierSchema idSchema = new TimelineIdentifierSchema.Builder()
                .addStringField(firstPKName).build();

        // table used for sync messages, with auto generated sequence id and ttl.
        TimelineSchema storeTimelineSchema = new TimelineSchema(storeTable, idSchema)
                .setSequenceIdColumnName(sequenceColumnName)
                .autoGenerateSeqId()
                .setTimeToLive(-1);

        storeService = serviceFactory.createTimelineStore(storeTimelineSchema);

        // table used to store messages, with manual set sequence id, long term storage and index.
        TimelineSchema syncTimelineSchema = new TimelineSchema(syncTable, idSchema)
                .setSequenceIdColumnName(sequenceColumnName)
                .autoGenerateSeqId()
                .setTimeToLive(30 * 24 * 3600);
        syncService = serviceFactory.createTimelineStore(syncTimelineSchema);
    }


    private static String generateLongContent() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1024 * 1024 * 1; i++) {
            builder.append('s');
        }
        return builder.toString();
    }


}
