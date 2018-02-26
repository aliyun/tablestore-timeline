package examples.scene;

import com.alicloud.openservices.tablestore.timeline.ScanParameter;
import com.alicloud.openservices.tablestore.timeline.ScanParameterBuilder;
import com.alicloud.openservices.tablestore.timeline.Timeline;
import com.alicloud.openservices.tablestore.timeline.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineStore;
import com.alicloud.openservices.tablestore.timeline.store.IStore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 基于Timeline LIB的微信朋友圈的简单实现。
 */
public class Moments {
    private final static String endpoint = "";
    private final static String accessKeyID = "";
    private final static String accessKeySecret = "";
    private final static String instanceName = "";
    private final static String storeTableName = "";
    private final static String syncTableName = "";


    public static void main(String[] args) {
        Moments weChat = new Moments();

        String lily = "user_lily";
        String lucy = "user_lucy";

        weChat.posts(lily, "是秋还是冬", new ArrayList<String>(), new ArrayList<String>());

        weChat.refresh(lucy, 0);

        weChat.close();
    }

    /**
     * 实现一个WeChatMessage，这里为了简单，直接继承StringMessage，且不增加功能，实际中需要考虑处理图片、视频、地理位置、屏蔽等。
     */
    public class WeChatMessage extends StringMessage {
        public WeChatMessage(String content) {
            super(content);
        }
    }

    private final static IStore store = new DistributeTimelineStore(
            new DistributeTimelineConfig(endpoint,accessKeyID,accessKeySecret, instanceName, storeTableName));
    private final static IStore sync = new DistributeTimelineStore(
            new DistributeTimelineConfig(endpoint,accessKeyID,accessKeySecret, instanceName, syncTableName));

    /**
     * 发布一条状态。
     */
    public void posts(String userId, String content, List<String> allowUsers, List<String> forbidUsers) {
        /**
         * 获取需要发布的用户列表
         */

        List<String> users = getUsersForPost(allowUsers, forbidUsers);

        /**
         * 构造消息对象
         */
        IMessage message = new WeChatMessage("是秋还是冬");

        /**
         * 写入自己的历史状态中
         */
        Timeline timeline = new Timeline(userId, store);
        timeline.store(message);

        /**
         * 发送给朋友圈好友
         */
        for (String user: users) {
            Timeline timeline2 = new Timeline(user, sync);
            timeline2.store(message);
        }
    }

    /**
     * 用户刷新自己的朋友圈。
     */
    public void refresh(String user, long lastSequenceID) {
        Timeline timeline = new Timeline(user, sync);

        /**
         * 构造读取的范围，逆向读取，从最新的数据开始一直读到上次的最新位置，每次返回100个。
         */
        ScanParameter scanParameter = ScanParameterBuilder
                .scanBackward()
                .from(Long.MAX_VALUE)
                .to(lastSequenceID)
                .maxCount(100)
                .build();

        Iterator<TimelineEntry> iterator = timeline.scan(scanParameter);
        while (iterator.hasNext()) {
            TimelineEntry entry = iterator.next();
            /**
             * 显示TimelineEntry内容。
             */
        }
    }

    public void close() {
        store.close();
        sync.close();
    }

    private List<String> getUsersForPost(List<String> allows, List<String> forbids) {
        List<String> allUsers = getAllFriends();
        for (String user: allUsers) {
            if (!forbids.contains(user)) {
                allows.add(user);
            }
        }
        return allows;
    }

    private List<String> getAllFriends() {
        /**
         * 获取朋友列表，这里省略实现。
         * 如果是使用Table Store，则一个getRange就可以查询出所有好友列表。
         */
        return new ArrayList<String>();
    }
}
