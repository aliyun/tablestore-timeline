package examples.v2;

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

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FitForTimelineV1 {
    private static String endpoint;
    private static String accessKeyId;
    private static String accessKeySecret;
    private static String instanceName;
    private static String storeTable = "storeTableV2";
    private static String syncTable = "syncTableV2";

    private static String firstPKName = "timelineId";

    private DistributeTimelineStore store = null;
    private DistributeTimelineStore sync = null;

    private TimelineStore storeService = null;
    private TimelineStore syncService = null;


    public FitForTimelineV1() {
        try {
            Conf conf = Conf.newInstance(System.getProperty("user.home") + "/timelineConf.json");
            endpoint = conf.getEndpoint();
            accessKeyId = conf.getAccessId();
            accessKeySecret = conf.getAccessKey();
            instanceName = conf.getInstanceName();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        FitForTimelineV1 demo = new FitForTimelineV1();
        boolean wantClean = false;

        demo.initTimelineV1();

        if (wantClean) {
            demo.dropTablesByV1();
        } else {
            demo.storeV1MessageByV1();

            demo.initTimelineV2();
            demo.readV1MessageByV2();

            demo.sendGroupMessageV2();
            demo.readV2MessageByV1();

            demo.updateV1MessageByV2();
        }
        //        demo.dropTablesByV1();
    }

    private void initTimelineV1() {
        DistributeTimelineConfig storeConfig = new DistributeTimelineConfig(endpoint, accessKeyId, accessKeySecret, instanceName, storeTable);
        storeConfig.setFirstPKName(firstPKName);
        storeConfig.setTtl(365 * 24 * 3600); // one Year
        storeConfig.setLimit(100);

        DistributeTimelineConfig syncConfig = new DistributeTimelineConfig(endpoint, accessKeyId, accessKeySecret, instanceName, syncTable);
        syncConfig.setFirstPKName(firstPKName);
        syncConfig.setTtl(30 * 24 * 3600); // one Month
        syncConfig.setLimit(100);

        store = new DistributeTimelineStore(storeConfig);
        sync = new DistributeTimelineStore(syncConfig);

        store.create();
        sync.create();
    }


    private void initTimelineV2() {
        TimelineStoreFactory serviceFactory = new TimelineStoreFactoryImpl(
                new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName)
        );

        //the pkName of the first PrimaryKey should be the same as vi timelineId
        TimelineIdentifierSchema idSchema = new TimelineIdentifierSchema.Builder()
                .addStringField(firstPKName).build();

        // table used for sync messages, with auto generated sequence id and ttl.
        TimelineSchema storeTimelineSchema = new TimelineSchema(storeTable, idSchema)
                .setSequenceIdColumnName("sequence_id")
                .autoGenerateSeqId()
                .setTimeToLive(365 * 24 * 3600);

        storeService = serviceFactory.createTimelineStore(storeTimelineSchema);

        // table used to store messages, with manual set sequence id, long term storage and index.
        TimelineSchema syncTimelineSchema = new TimelineSchema(syncTable, idSchema)
                .setSequenceIdColumnName("sequence_id")
                .autoGenerateSeqId()
                .setTimeToLive(30 * 24 * 3600);
        syncService = serviceFactory.createTimelineStore(syncTimelineSchema);
    }

    private void storeV1MessageByV1() {
        System.out.println("[Send message by v1]");
        List<String> groupMembers = Arrays.asList("user_A_v1", "user_B_v1", "user_C_v1");

        sendGroupMessageV1("group_v1", new StringMessage("user_B_v1:阿里云的NoSQL数据库是哪个?"), groupMembers);
        sendGroupMessageV1("group_v1", new StringMessage("user_C_v1:是表格存储"), groupMembers);
        sendGroupMessageV1("group_v1", new StringMessage("user_B_v1:好，谢谢"), groupMembers);
    }

    private void sendGroupMessageV1(String groupName, IMessage message, List<String> groupMembers) {
        com.alicloud.openservices.tablestore.timeline.Timeline sender = new com.alicloud.openservices.tablestore.timeline.Timeline(groupName, store);
        sender.store(message);

        for (String user : groupMembers) {
            com.alicloud.openservices.tablestore.timeline.Timeline receiver = new com.alicloud.openservices.tablestore.timeline.Timeline(user, sync);
            receiver.store(message);
        }
    }

    private void readV1MessageByV2() {
        System.out.println("[Read message by v2 api which is stored by v1]");

        System.out.println("\t[Read Group]: group_v1");
        TimelineIdentifier group = new TimelineIdentifier.Builder()
                .addField(firstPKName, "group_v1")
                .build();

        TimelineQueue storeTimelineQueue = storeService.createTimelineQueue(group);

        Iterator<TimelineEntry> storeIterator = storeTimelineQueue.scan(new ScanParameter().scanBackward(Long.MAX_VALUE));
        while (storeIterator.hasNext()) {
            TimelineEntry entry = storeIterator.next();
            TimelineMessageForV1 message = new TimelineMessageForV1(entry.getMessage());
            System.out.println(String.format("\t\t[SequenceId]%s, [MessageId]%s, [Content]%s", entry.getSequenceID(), message.getMessageID(), message.getContent()));
        }

        System.out.println("\t[Read User]: user_A_v1");
        TimelineIdentifier user = new TimelineIdentifier.Builder()
                .addField(firstPKName, "user_A_v1")
                .build();
        TimelineQueue syncTimelineQueue = syncService.createTimelineQueue(user);

        Iterator<TimelineEntry> syncIterator = syncTimelineQueue.scan(new ScanParameter().scanBackward(Long.MAX_VALUE));
        while (syncIterator.hasNext()) {
            TimelineEntry entry = syncIterator.next();
            TimelineMessageForV1 message = new TimelineMessageForV1(entry.getMessage());
            System.out.println(String.format("\t\t[SequenceId]%s, [MessageId]%s, [Content]%s", entry.getSequenceID(), message.getMessageID(), message.getContent()));
        }
    }

    private void sendGroupMessageV2() {
        System.out.println("[Store message by v2 which is compatible with v1]");
        List<String> groupMembers = Arrays.asList("user_A_v2", "user_B_v2", "user_C_v2");

        sendMessageV1ByV2("group_v2", new TimelineMessageForV1("user_B_v2:阿里云的NoSQL数据库是哪个?"), groupMembers);
        sendMessageV1ByV2("group_v2", new TimelineMessageForV1("user_C_v2:是表格存储"), groupMembers);
        sendMessageV1ByV2("group_v2", new TimelineMessageForV1("user_B_v2:好，谢谢"), groupMembers);
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

    private void readV2MessageByV1() {
        System.out.println("[Read message by v1 api which stored by v2]");

        System.out.println("\t[Read Group]: group_v2");
        com.alicloud.openservices.tablestore.timeline.Timeline group = new com.alicloud.openservices.tablestore.timeline.Timeline("group_v2", store);
        com.alicloud.openservices.tablestore.timeline.ScanParameter scanParameter = ScanParameterBuilder
                .scanBackward()
                .from(Long.MAX_VALUE)
                .to(0)
                .maxCount(100)
                .build();
        Iterator<com.alicloud.openservices.tablestore.timeline.TimelineEntry> entries = group.scan(scanParameter);
        while (entries.hasNext()) {
            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry = entries.next();
            System.out.println(String.format("\t\t[SequenceId]%s, [MessageId]%s, [Content]%s", entry.getSequenceID(), entry.getMessage().getMessageID(), new String(entry.getMessage().serialize())));
        }

        System.out.println("\t[Read User]: user_B_v2");
        com.alicloud.openservices.tablestore.timeline.Timeline user = new com.alicloud.openservices.tablestore.timeline.Timeline("user_B_v2", sync);
        com.alicloud.openservices.tablestore.timeline.ScanParameter userScanParameter = ScanParameterBuilder
                .scanBackward()
                .from(Long.MAX_VALUE)
                .to(0)
                .maxCount(100)
                .build();
        Iterator<com.alicloud.openservices.tablestore.timeline.TimelineEntry> userEntries = user.scan(userScanParameter);
        while (userEntries.hasNext()) {
            com.alicloud.openservices.tablestore.timeline.TimelineEntry entry = userEntries.next();
            System.out.println(String.format("\t\t[SequenceId]%s, [MessageId]%s, [Content]%s", entry.getSequenceID(), entry.getMessage().getMessageID(), new String(entry.getMessage().serialize())));
        }
    }

    private void updateV1MessageByV2() {
        System.out.println("[Update message by v2 api which stored by v1]");

        System.out.println("\t[Before]: read group before update group_v1");
        TimelineIdentifier group = new TimelineIdentifier.Builder()
                .addField(firstPKName, "group_v1")
                .build();
        TimelineQueue storeTimelineQueue = storeService.createTimelineQueue(group);

        TimelineEntry timelineEntryBefore = storeTimelineQueue.getLatestTimelineEntry();
        long sequenceId = timelineEntryBefore.getSequenceID();

        TimelineMessageForV1 messageBefore = new TimelineMessageForV1(timelineEntryBefore.getMessage());
        System.out.println(String.format("\t\t[SequenceId]:%s, [MessageId]:%s, [Content]:%s",
                timelineEntryBefore.getSequenceID(), messageBefore.getMessageID(), messageBefore.getContent()));

        messageBefore.setMessageId("updatedMessageId");
        messageBefore.setContent(messageBefore.getContent() + "(更新)");

        System.out.println(String.format("\t[Update Group]: group_v1, [SequenceId]:%s", sequenceId));
        storeTimelineQueue.update(sequenceId, messageBefore.getTimelineMessage());

        TimelineEntry timelineEntryAfter = storeTimelineQueue.get(sequenceId);

        System.out.println("\t[After]: read group after update group_v1");
        TimelineMessageForV1 messageAfter = new TimelineMessageForV1(timelineEntryAfter.getMessage());
        System.out.println(String.format("\t\t[SequenceId]:%s, [MessageId]:%s, [Content]:%s",
                timelineEntryAfter.getSequenceID(), messageAfter.getMessageID(), messageAfter.getContent()));
    }

    private void dropTablesByV1() {
        store.drop();
        sync.drop();
    }
}
