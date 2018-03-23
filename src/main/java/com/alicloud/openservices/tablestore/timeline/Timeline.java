package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.timeline.common.TimelineCallback;
import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.common.TimelineExceptionType;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.store.IStore;

import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * Timeline 类定义，提供读写等接口。
 * 使用Timeline LIB时应该调用Timeline的接口，而不是IStore的接口。
 */
public class Timeline {
    /**
     * 每个Timeline的ID信息。
     * 在IM中，每个回话或者群组是一个Timeline。
     * 在Feed流中，每个用户的个人页或者关注页是一个Timeline。
     */
    private String timelineID = null;

    /**
     * 此Timeline绑定的Store层。不同的Timeline可以采用不同的Store。
     * 存储Timeline使用一个独立Store，此Store可以采用SSD+SATA存储介质，价格低廉，对应于Table Store的容量性实例。
     * 同步Timeline使用一个Timeline，此Store可以采用SSD存储介质，读性能极佳，对应于Table Store的高性能实例。
     */
    private IStore store = null;

    /**
     * Timeline的构造函数。
     * @param timelineID    此Timeline对应的ID。
     * @param store         此Timeline关联的Store，一般为存储Store或同步Store。
     */
    public Timeline(String timelineID, IStore store) {
        if (timelineID == null || timelineID.isEmpty()) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "Timeline parameter timelineID is null or empty");
        }

        if (store == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "Timeline parameter store is null");
        }

        this.timelineID = timelineID;
        this.store = store;
    }

    /**
     * 写入一个消息到此Timeline中。
     * @param message   消息对象，需实现IMessage接口。
     * @return          完整的TimelineEntry，包括消息和顺序ID。
     */
    public TimelineEntry store(IMessage message) {
        if (message == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "store parameter message is null");
        }

        return this.store.write(this.timelineID, message);
    }

    /**
     * 异步写入消息接口。
     * @param message     消息对象，需实现IMessage接口。
     * @param callback    回调函数。
     * @return            Future对象。
     */
    public Future<TimelineEntry> storeAsync(IMessage message, TimelineCallback<IMessage> callback) {
        if (message == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "storeAsync parameter message is null");
        }

        return this.store.writeAsync(this.timelineID, message, callback);
    }

    /**
     * 批量写入消息接口。
     * 此接口只是把消息加入到本地的一个buffer中，当buffer满或者超时（默认10s，可配置）才会统一写入。
     * 此接口返回时并不一定消息已经写入成功。
     * @param message 消息对象，需实现IMessage接口。
     */
    public void batch(IMessage message) {
        if (message == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "store parameter message is null");
        }

        this.store.batch(this.timelineID, message);
    }

    /**
     * 同步更新消息。
     * @param sequenceID  消息顺序ID，和TimelineID一起唯一确定一条消息。
     * @param message     消息对象，需实现IMessage接口。
     * @return            完整的TimelineEntry，包括消息和顺序ID。
     */
    public TimelineEntry update(Long sequenceID, IMessage message) {
        if (message == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "store parameter message is null");
        }

        if (sequenceID == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "store parameter sequenceID is null");
        }

        return this.store.update(this.timelineID, sequenceID, message);
    }

    /**
     * 异步更新消息接口。
     * @param sequenceID  消息顺序ID，和TimelineID一起唯一确定一条消息。
     * @param message     消息对象，需实现IMessage接口。
     * @param callback    回调函数。
     * @return            Future对象。
     */
    public Future<TimelineEntry> updateAsync(Long sequenceID, IMessage message, TimelineCallback<IMessage> callback) {
        if (message == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "store parameter message is null");
        }

        if (sequenceID == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "store parameter sequenceID is null");
        }

        return this.store.updateAsync(this.timelineID, sequenceID, message, callback);
    }

    /**
     * 同步读取接口，通过制定一个唯一的顺序ID读取目标TimelineEntry。
     * @param sequenceID    顺序ID。
     * @return              完整的TimelineEntry，包括消息和顺序ID。
     */
    public TimelineEntry get(Long sequenceID) {
        if (sequenceID == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "get parameter sequenceID is null");
        }

        return this.store.read(this.timelineID, sequenceID);
    }

    /**
     * 异步读取接口，通过制定一个唯一的顺序ID读取目标TimelineEntry。
     * @param sequenceID    顺序ID。
     * @param callback      读取结束后的回调函数。
     * @return              Future对象。
     */
    public Future<TimelineEntry> getAsync(Long sequenceID, TimelineCallback<Long> callback) {
        if (sequenceID == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "getAsync parameter sequenceID is null");
        }

        return this.store.readAsync(this.timelineID, sequenceID, callback);
    }

    /**
     * 顺序读取一段范围内或固定数目的消息，支持逆序，正序。
     * @param parameter     顺序读取的参数，包括方向、from、to和maxCount。
     * @return              TimelineEntry的迭代器，通过迭代器可以遍历到待读取的所有消息。
     */
    public Iterator<TimelineEntry> scan(ScanParameter parameter) {
        if (parameter == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "scan parameter is null");
        }

        return this.store.scan(this.timelineID, parameter);
    }
}
