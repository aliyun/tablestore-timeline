package examples.v2;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline2.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.*;

import com.alicloud.openservices.tablestore.timeline2.query.*;

import java.util.Iterator;

public class TimelineV2 {
    public static void main(String[] args) {
        SyncClient client = new SyncClient("http://timelinev2.cn-shanghai.ots.aliyuncs.com",
                "*************", "************************", "timelinev2");
        TimelineStoreFactory storeFactory = new TimelineStoreFactoryImpl(client);

        TimelineIdentifierSchema idSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("timeline_id").build();
        TimelineMetaStore groupMeta = initGroupMetaTable(storeFactory, idSchema, false);
        TimelineStore storeService = initStoreTable(storeFactory, idSchema, false);
        TimelineStore syncService = initSyncTable(storeFactory, idSchema, false);

        try {

            // create new groups
            createGroups(groupMeta);
            TimelineMeta group = readGroup(groupMeta,
                    new TimelineIdentifier.Builder().addField("timeline_id", "group_666").build());
            System.out.println(group);

            // search groups by group name
            SearchResult<TimelineMeta> metas = searchGroup(groupMeta, "tablestore");
            System.out.println("Search results: ");
            System.out.println("Total count: " + metas.getTotalCount());
            System.out.println("Return count: " + metas.getEntries().size());
            for (SearchResult.Entry<TimelineMeta> entry : metas.getEntries()) {
                System.out.println(entry.getData());
            }

            // send message in group and sync to all group members
            boolean sendMessages = false;
            if (sendMessages) {
                String[] groupMembers = new String[]{"user_a", "user_b", "user_c", "user_d", "user_e"};
                long messageId = 0;
                messageId = sendGroupMessage(storeService, syncService, group, groupMembers, messageId, "hello world");
                messageId = sendGroupMessage(storeService, syncService, group, groupMembers, messageId, "where are you");
                messageId = sendGroupMessage(storeService, syncService, group, groupMembers, messageId, "who are you");
                messageId = sendGroupMessage(storeService, syncService, group, groupMembers, messageId, "what are you doing");
                messageId = sendGroupMessage(storeService, syncService, group, groupMembers, messageId, "just kidding");
            }

            // receive new messages from user's receive box
            long lastSeqId = 0;
            receiveNewMessages(syncService, "user_a", lastSeqId);

            // search messages
            searchMessage(storeService, "user_a", "you");
        } finally {
            storeService.flush();
            syncService.flush();
            storeService.close();
            syncService.close();

            client.shutdown();
        }
    }

    private static void createGroups(TimelineMetaStore groupMeta) {
        int groupCount = 10;
        for (int i = 0; i < groupCount; i++) {
            createNewGroup(groupMeta, i);
        }
    }

    private static SearchResult<TimelineMeta> searchGroup(TimelineMetaStore groupMeta, String text) {
        FieldCondition condition =
                or(
                        field("group_name").equals(text)
                );
        SearchParameter param = new SearchParameter(condition)
                .limit(5)
                .calculateTotalCount();
        return groupMeta.search(param);
    }

    private static TimelineMeta readGroup(TimelineMetaStore groupMeta, TimelineIdentifier identifier) {
        return groupMeta.read(identifier);
    }

    private static void searchMessage(TimelineStore storeService, String user, String text) {
        System.out.println("Search messages of '" + user + "': " + text);
        FieldCondition condition =
                and(
                    field("receivers").equals(user),
                    field("text").match(text)
                );

        SearchParameter searchParameter = new SearchParameter(condition).limit(100).calculateTotalCount().
                orderBy(new String[]{"timestamp"}, SortOrder.DESC);
        SearchResult<TimelineEntry> result = storeService.search(searchParameter);

        for (SearchResult.Entry<TimelineEntry> entry : result.getEntries()) {
            System.out.println(entry.getIdentifier() + ": " + entry.getData().getMessage());
        }
    }

    private static long receiveNewMessages(TimelineStore syncService, String user, long seqId) {
        TimelineQueue receiveBox = syncService.createTimelineQueue(new TimelineIdentifier.Builder().addField("timeline_id", user).build());
        ScanParameter param = new ScanParameter().scanForward(seqId).maxCount(100);
        Iterator<TimelineEntry> entries = receiveBox.scan(param);
        long lastSeqId = 0;
        while (entries.hasNext()) {
            TimelineEntry entry = entries.next();
            System.out.println(String.format("%d : %s", entry.getSequenceID(), entry.getMessage().getString("text")));
            lastSeqId = entry.getSequenceID();
        }

        return lastSeqId;
    }

    private static long sendGroupMessage(TimelineStore storeService, TimelineStore syncService,
                                         TimelineMeta group, String[] groupMembers,
                                         long lastMessageId, String message) {
        System.out.println("Send message: " + message);
        long messageId = MessageIdGenerator.next(lastMessageId);
        long sendTime = System.currentTimeMillis();
        TimelineMessage tm = new TimelineMessage()
                .setField("text", message)
                .setField("timestamp", sendTime)
                .setField("message_id", messageId)
                .setField("receivers", groupMembers);

        // store message to group's send box, and set sequence id manually
        TimelineQueue groupTimelineQueue = storeService.createTimelineQueue(group.getIdentifier());
        groupTimelineQueue.batchStore(tm.getLong("message_id"), tm);

        // sync message to user's receive box
        TimelineMessage tm2 = new TimelineMessage()
                .setField("text", message)
                .setField("timestamp", sendTime)
                .setField("message_id", messageId);
        for (String user : groupMembers) {
            TimelineQueue userTimelineQueue = syncService.createTimelineQueue(
                    new TimelineIdentifier.Builder().addField("timeline_id", user).build());
            userTimelineQueue.batchStore(tm2);
        }

        return messageId;
    }

    private static TimelineMeta createNewGroup(TimelineMetaStore groupMeta, int i) {
        TimelineMeta group = new TimelineMeta(
                new TimelineIdentifier.Builder().addField("timeline_id", "group_" + i).build())
                .setField("group_name", "tablestore developers")
                .setField("create_time", System.currentTimeMillis());
        groupMeta.insert(group);
        return group;
    }

    private static TimelineStore initSyncTable(TimelineStoreFactory serviceFactory, TimelineIdentifierSchema idSchema, boolean prepareTable) {
        // table used for sync messages, with auto generated sequence id and ttl.
        TimelineSchema timelineSchema = new TimelineSchema("syncTable", idSchema)
                .autoGenerateSeqId()
                .setTimeToLive(86400 * 7);

        TimelineStore service = serviceFactory.createTimelineStore(timelineSchema);
        if (prepareTable) {
            service.prepareTables();
        }
        return service;
    }

    private static TimelineMetaStore initGroupMetaTable(TimelineStoreFactory serviceFactory, TimelineIdentifierSchema idSchema, boolean prepareTable) {
        // index schema of meta table, take group meta for example
        IndexSchema metaIndex = new IndexSchema();
        metaIndex.addFieldSchema(new FieldSchema("group_name", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));

        // set timeline schema and prepare all tables include data table/index and meta table/index.
        TimelineMetaSchema metaSchema = new TimelineMetaSchema("groupMeta", idSchema)
                .withIndex("metaIndex", metaIndex);
        TimelineMetaStore service = serviceFactory.createMetaStore(metaSchema);
        if (prepareTable) {
            service.prepareTables();
        }
        return service;
    }

    private static TimelineStore initStoreTable(TimelineStoreFactory serviceFactory, TimelineIdentifierSchema idSchema, boolean prepareTable) {
        // index schema of data table, take message full text index for example
        IndexSchema dataIndex = new IndexSchema();
        dataIndex.addFieldSchema(new FieldSchema("text", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        dataIndex.addFieldSchema(new FieldSchema("receivers", FieldType.KEYWORD).setIsArray(true).setIndex(true));
        dataIndex.addFieldSchema(new FieldSchema("timestamp", FieldType.LONG).setEnableSortAndAgg(true));

        // table used to store messages, with manual set sequence id, long term storage and index.
        TimelineSchema timelineSchema = new TimelineSchema("storeTable", idSchema)
                .manualSetSeqId()
                .withIndex("dataIndex", dataIndex);
        TimelineStore service = serviceFactory.createTimelineStore(timelineSchema);
        if (prepareTable) {
            service.prepareTables();
        }
        return service;
    }
}
