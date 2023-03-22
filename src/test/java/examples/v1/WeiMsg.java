package examples.v1;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timeline.ScanParameter;
import com.alicloud.openservices.tablestore.timeline.ScanParameterBuilder;
import com.alicloud.openservices.tablestore.timeline.Timeline;
import com.alicloud.openservices.tablestore.timeline.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineStore;
import com.alicloud.openservices.tablestore.timeline.store.IStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeMap;

public class WeiMsg {

    /**
     * 阿里云的秘钥配置
     * url: https://ram.console.aliyun.com/manage/ak
     */
    private static final String ACCESS_KEY_ID = "<input your access key id>";
    private static final String ACCESS_KEY_SECRET = "<input your access key secret>";

    /**
     * 阿里云表格存储Tablestore的实例信息，包括访问地址和实例名
     * url: https://www.aliyun.com/product/ots
     */
    private static final String ENDPOINT = "<input your tablestore endpoint>";
    private static final String INSTANCE_NAME = "<input your tablestore instance name>";

    private static final SyncClient tableStore = new SyncClient(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET, INSTANCE_NAME);

    /**
     * 当前示例中涉及的表，包括了存储表，同步表，用户表， 关注表和粉丝索引。
     */
    private static final String STORE_TABLE_NAME = "store_table";
    private static final String SYNC_TABLE_NAME = "sync_table";
    private static final String USER_TABLE_NAME = "user_table";
    private static final String WATCH_TABLE_NAME = "watch_table";
    private static final String FOLLOW_INDEX_NAME = "follow_index";

    /**
     * 创建存储表和同步表对应的 Timeline Store
     */
    private static final IStore weiMsgStoreStore = new DistributeTimelineStore(new DistributeTimelineConfig(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET,
            INSTANCE_NAME, STORE_TABLE_NAME));
    private static final IStore weiMsgSyncStore = new DistributeTimelineStore(new DistributeTimelineConfig(ENDPOINT, ACCESS_KEY_ID, ACCESS_KEY_SECRET,
            INSTANCE_NAME, SYNC_TABLE_NAME));

    public static void main(String[] args) throws InterruptedException {
        WeiMsg weiMsg = new WeiMsg();

        // 系统初始化，创建辅助表，包括用户表，关注表等。
        weiMsg.init();

        // 注册新用户
        weiMsg.registerUsers("id_1", "lucy", false);
        weiMsg.registerUsers("id_2", "lily", false);
        weiMsg.registerUsers("id_3", "lilei", false);
        weiMsg.registerUsers("id_4", "hanmeimei", true);

        // 用户之间相互关注
        weiMsg.watchUser("id_1", "id_4");
        weiMsg.watchUser("id_1", "id_2");
        weiMsg.watchUser("id_2", "id_4");
        weiMsg.watchUser("id_3", "id_4");
        weiMsg.watchUser("id_4", "id_3");

        // 关注表和粉丝表的关系是：索引关系，索引使用了最终一致性的全局二级索引，所以这里需要稍微等一点时间才能保证索引表（粉丝表）写成功。
        Thread.sleep(2000);

        // 发送一条微消息
        weiMsg.post("id_1", "lucy's new message", "2023-01-15 11:12:40");
        weiMsg.post("id_2", "lily's new message", "2023-01-17 05:23:11");
        weiMsg.post("id_3", "lilei's new message", "2023-01-23 14:09:45");
        weiMsg.post("id_4", "hanmeimei's new message", "2023-01-31 11:11:11");

        // lucy 刷新获取新消息
        weiMsg.refresh("id_1");

        weiMsg.close();
    }

    private void init() {
        // 创建用户表
        {
            TableMeta tableMeta = new TableMeta(USER_TABLE_NAME);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("user_id", PrimaryKeyType.STRING));

            int timeToLive = -1;
            int maxVersions = 1;
            TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);

            tableStore.createTable(request);
        }

        // 创建关注表和粉丝表
        {
            TableMeta tableMeta = new TableMeta(WATCH_TABLE_NAME);
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("user_id", PrimaryKeyType.STRING));
            tableMeta.addPrimaryKeyColumn(new PrimaryKeySchema("watch_user_id", PrimaryKeyType.STRING));

            int timeToLive = -1;
            int maxVersions = 1;

            TableOptions tableOptions = new TableOptions(timeToLive, maxVersions);

            ArrayList<IndexMeta> indexMetas = new ArrayList<IndexMeta>();
            IndexMeta indexMeta = new IndexMeta(FOLLOW_INDEX_NAME);
            indexMeta.addPrimaryKeyColumn("watch_user_id");
            indexMeta.addPrimaryKeyColumn("user_id");
            indexMetas.add(indexMeta);

            CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions, indexMetas);

            tableStore.createTable(request);
        }

        // 创建存储库和同步库
        weiMsgStoreStore.create();
        weiMsgSyncStore.create();
    }

    private void close() {
        weiMsgStoreStore.close();
        weiMsgSyncStore.close();
        tableStore.shutdown();
    }

    /**
     * 注册新用户，本质是在用户表里面新增加一行
     * @param userID 新注册的用户ID
     * @param nickName 新用户的昵称
     * @param isBigUser 是否是大用户，实际使用过程中，这个属性可以通过新增 UpdateUserInfo 更新
     */
    private void registerUsers(String userID, String nickName, boolean isBigUser) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.fromString(userID));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(USER_TABLE_NAME, primaryKey);

        rowPutChange.addColumn(new Column("nick_name", ColumnValue.fromString(nickName)));
        rowPutChange.addColumn(new Column("is_big_user", ColumnValue.fromBoolean(isBigUser)));

        tableStore.putRow(new PutRowRequest(rowPutChange));
    }

    /**
     * 关注用户，本质是在关注表里面新写入一行，同时粉丝索引里面会自动更新
     * @param userID  用户ID
     * @param watchUserID  新关注的用户ID
     */
    private void watchUser(String userID, String watchUserID) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.fromString(userID));
        primaryKeyBuilder.addPrimaryKeyColumn("watch_user_id", PrimaryKeyValue.fromString(watchUserID));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(WATCH_TABLE_NAME, primaryKey);

        tableStore.putRow(new PutRowRequest(rowPutChange));
    }

    /**
     * 发送一条微消息
     * @param userID 发送微消息的用户ID
     * @param content 微消息的内容
     * @param time  发送时间
     */
    private void post(String userID, String content, String time) {
        if (isBigUser(userID)) {
            postBigUser(userID, content, time);
        } else {
            postNormalUser(userID, content, time);
        }
    }

    /**
     * 判断是否是大用户，本质是读取用户表，判断用户的属性列中的 is_big_user 是否为 TRUE
     * @param userID 用户ID
     * @return  如果是大用户 则返回TRUE，否则为 FALSE
     */
    private boolean isBigUser(String userID) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.fromString(userID));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(USER_TABLE_NAME, primaryKey);
        criteria.setMaxVersions(1);

        GetRowResponse getRowResponse = tableStore.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();
        return row.getColumn("is_big_user").get(0).getValue().asBoolean();
    }

    /**
     * 如果是大用户的发送微消息的流程，只需要写入自己的存储表中就行，因为这里会使用读扩散
     * @param userID 用户ID
     * @param content 微消息内容
     * @param time  发送时间
     */
    private void postBigUser(String userID, String content, String time) {
        StringMessage message = new StringMessage(content);
        message.addAttribute("author", userID);
        message.addAttribute("time", time);

        // 写入自己的历史库中
        Timeline timeline = new Timeline(userID, weiMsgStoreStore);
        timeline.store(message);
    }

    /**
     * 普通用户发送微消息的流程，不仅需要写入自己的存储表，还需要写入粉丝的同步表中，这里使用写扩散
     * @param userID 用户ID
     * @param content 微消息内容
     * @param time  发送时间
     */
    private void postNormalUser(String userID, String content, String time) {
        StringMessage message = new StringMessage(content);
        message.addAttribute("author", userID);
        message.addAttribute("time", time);

        // 写入自己的历史库中
        {
            Timeline timeline = new Timeline(userID, weiMsgStoreStore);
            timeline.store(message);
        }

        // 写入粉丝的收件箱中
        {
            ArrayList<String> follows = getFollowUserIDs(userID);

            for (String user : follows) {
                Timeline timeline2 = new Timeline(user, weiMsgSyncStore);
                timeline2.store(message);
            }
        }
    }

    /**
     * 刷新微消息首页，也就是获取关注人的最新微消息内容
     * @param userID  刷新微消息的用户ID
     */
    private void refresh(String userID) {
        TreeMap<Long, StringMessage> messages = new TreeMap<Long, StringMessage>();

        // 读取非大用户消息
        {
            Timeline timeline = new Timeline(userID, weiMsgSyncStore);

            ScanParameter scanParameter = ScanParameterBuilder
                    .scanBackward()
                    .from(Long.MAX_VALUE)
                    .to(0) // 这里的sequenceID 简化为0了，实际生产过程中需要从客户端返回非大用户最新读取到的位置或者用一张表单独存储
                    .maxCount(1000)
                    .build();

            Iterator<TimelineEntry> iterator = timeline.scan(scanParameter);
            while (iterator.hasNext()) {
                TimelineEntry entry = iterator.next();
                messages.put(entry.getSequenceID(), (StringMessage)entry.getMessage());
            }
        }

        // 读取大用户消息
        ArrayList<String> bigWatchUsers = getBigUserInWatch(userID);
        for (String user: bigWatchUsers) {
            Timeline daVTimeline = new Timeline(user, weiMsgStoreStore);
            ScanParameter scanParameter2 = ScanParameterBuilder
                    .scanBackward()
                    .from(Long.MAX_VALUE)
                    .to(0) // 这里的sequenceID 简化为0了，实际生产过程中需要从客户端返回该大用户最新读取到的位置或者用一张表单独存储
                    .maxCount(1000)
                    .build();

            Iterator<TimelineEntry> iterator2 = daVTimeline.scan(scanParameter2);
            while (iterator2.hasNext()) {
                TimelineEntry entry2 = iterator2.next();
                messages.put(entry2.getSequenceID(), (StringMessage)entry2.getMessage());
            }
        }

        // 打印消息
        Collection<StringMessage> messageList = messages.values();
        for(StringMessage message : messageList){
            System.out.println("author: " + message.getAttributes().get("author") +
                    "\ttime: " + message.getAttributes().get("time") +
                    "\tcontent: " + message.getContent());
        }
    }

    /**
     * 获取粉丝列表
     * @param userID 获取该UserID 的粉丝列表
     * @return  粉丝列表
     */
    private ArrayList<String> getFollowUserIDs(String userID) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(FOLLOW_INDEX_NAME);

        // 设置起始主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn("watch_user_id", PrimaryKeyValue.fromString(userID));
        startPrimaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 设置结束主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn("watch_user_id", PrimaryKeyValue.fromString(userID));
        endPrimaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        ArrayList<String> users = new ArrayList<String>();
        while (true) {
            GetRangeResponse getRangeResponse = tableStore.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                users.add(row.getPrimaryKey().getPrimaryKeyColumn("user_id").getValue().asString());
            }

            // 若nextStartPrimaryKey不为null, 则继续读取.
            if (getRangeResponse.getNextStartPrimaryKey() != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
            } else {
                break;
            }
        }

        return users;
    }

    /**
     * 判断关注人中是否有大用户
     * @param userID 用户ID
     * @return 返回关注人中大用户的列表
     */
    private ArrayList<String> getBigUserInWatch(String userID) {
        RangeRowQueryCriteria rangeRowQueryCriteria = new RangeRowQueryCriteria(WATCH_TABLE_NAME);

        // 设置起始主键
        PrimaryKeyBuilder startPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        startPrimaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.fromString(userID));
        startPrimaryKeyBuilder.addPrimaryKeyColumn("watch_user_id", PrimaryKeyValue.INF_MIN);
        rangeRowQueryCriteria.setInclusiveStartPrimaryKey(startPrimaryKeyBuilder.build());

        // 设置结束主键
        PrimaryKeyBuilder endPrimaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        endPrimaryKeyBuilder.addPrimaryKeyColumn("user_id", PrimaryKeyValue.fromString(userID));
        endPrimaryKeyBuilder.addPrimaryKeyColumn("watch_user_id", PrimaryKeyValue.INF_MAX);
        rangeRowQueryCriteria.setExclusiveEndPrimaryKey(endPrimaryKeyBuilder.build());

        rangeRowQueryCriteria.setMaxVersions(1);

        ArrayList<String> users = new ArrayList<String>();
        while (true) {
            GetRangeResponse getRangeResponse = tableStore.getRange(new GetRangeRequest(rangeRowQueryCriteria));
            for (Row row : getRangeResponse.getRows()) {
                String watchUser = row.getPrimaryKey().getPrimaryKeyColumn("watch_user_id").getValue().asString();
                if (isBigUser(watchUser)) {
                    users.add(watchUser);
                }
            }

            // 若nextStartPrimaryKey不为null, 则继续读取.
            if (getRangeResponse.getNextStartPrimaryKey() != null) {
                rangeRowQueryCriteria.setInclusiveStartPrimaryKey(getRangeResponse.getNextStartPrimaryKey());
            } else {
                break;
            }
        }

        return users;
    }
}
