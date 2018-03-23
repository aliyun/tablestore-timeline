package com.alicloud.openservices.tablestore.timeline.store;

import com.alicloud.openservices.tablestore.timeline.ScanParameter;
import com.alicloud.openservices.tablestore.timeline.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline.common.TimelineCallback;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * 存储层接口的定义。
 */
public interface IStore extends Closeable {

    /**
     * 写一条消息到特定Timeline中。
     * @param timelineID   需要写入的Timeline的ID
     * @param message      需要写入的消息体
     * @return             写入成功的消息，包括顺序ID
     */
    TimelineEntry write(String timelineID, IMessage message);

    /**
     * 批量写入接口，性能相对于Write更好。
     * 在实现中，由于是等到一定数据量或者一定时间后才会统一提交，所以batch接口调用完成后并不一定写入成功。
     * 在程序结束的时候，需要调用close()接口flush buffer中剩余未提交的数据。
     * @param timelineID    需要写入的Timeline的ID
     * @param message       需要写入的消息体
     */
    void batch(String timelineID, IMessage message);

    /**
     * 异步写一条消息到特定Timeline中
     * @param timelineID   需要写入的Timeline的ID
     * @param message       需要写入的消息体
     * @param callback      回调函数
     * @return              Future对象
     */
    Future<TimelineEntry> writeAsync(String timelineID, IMessage message, TimelineCallback<IMessage> callback);

    /**
     * 同步更新Timeline中的某条消息的属性值。
     * @param timelineID   需要更新的Timeline的ID
     * @param sequenceID   需要更新的消息的sequenceID
     * @param message      需要更新的消息体
     * @return             写入成功的消息，包括顺序ID
     */
    TimelineEntry update(String timelineID, Long sequenceID, IMessage message);

    /**
     * 异步更新Timeline中的某条消息的属性值。
     * @param timelineID   需要更新的Timeline的ID
     * @param sequenceID   需要更新的消息的sequenceID
     * @param message      需要更新的消息体
     * @param callback     回调函数
     * @return             Future对象，写入成功的消息，包括顺序ID
     */
    Future<TimelineEntry> updateAsync(String timelineID,
                                      Long sequenceID,
                                      IMessage message,
                                      TimelineCallback<IMessage> callback);

    /**
     * 同步读取一个Timeline实体
     * @param timelineID     需要读取的Timeline的ID
     * @param sequenceID     需要读取的消息的顺序ID
     * @return               读取到的Timeline实体
     */
    TimelineEntry read(String timelineID, Long sequenceID);

    /**
     * 异步读取一个Timeline实体
     * @param timelineID     需要读取的Timeline的ID
     * @param sequenceID     需要读取的消息的顺序ID
     * @param callback       回调函数
     * @return               Future对象
     */
    Future<TimelineEntry> readAsync(String timelineID, Long sequenceID, TimelineCallback<Long> callback);

    /**
     * 读取固定数量的Timeline实体。对于不同的Timeline模型，读取的参数有差异。
     * 比如IM中读取历史消息，是逆序读，但是读取最新的同步消息是正序读。
     * @param timelineID     对应的Timeline ID，一般是用户ID，或群组ID
     * @param parameter      范围读取的参数对象，包括：direction、from、to和maxCount
     * @return               TimelineEntry的迭代器
     */
    Iterator<TimelineEntry> scan(String timelineID, ScanParameter parameter);

    /**
     * 创建store涉及到的资源，比如创建存储或同步系统中的表等，创建前必须判断是否已经存在。
     */
    void create();

    /**
     * 销毁store涉及到的资源，比如会删除存或同步系统中的表，此操作非常危险，一定要慎重。
     */
    void drop();

    /**
     * 判断store涉及到的资源是否已经被成功创建。
     * @return  True/False 是否存在
     */
    boolean exist();

    /**
     * 关闭store。
     */
    void close();
}
