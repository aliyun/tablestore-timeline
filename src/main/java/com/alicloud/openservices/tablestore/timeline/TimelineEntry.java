package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.timeline.message.IMessage;

/**
 * Timeline实体类，包括顺序ID和消息体。
 */
public class TimelineEntry {
    /**
     * 顺序（sequence）ID，保证严格递增。由LIB或存储层系统生成。LIB使用者无需创建。
     */
    private Long sequenceID = null;

    /**
     * 消息实体，需要自定义消息类，且实现IMessage接口。
     */
    private IMessage message = null;

    /**
     * 构造函数。
     * @param sequenceID    顺序ID，由LIB或存储系统生成。
     * @param message       消息实体。
     */
    public TimelineEntry(Long sequenceID, IMessage message) {
        this.sequenceID = sequenceID;
        this.message = message;
    }

    /**
     * 读取消息的顺序ID，此ID是严格递增的，由LIB或存储系统提供。不需要使用者设置。
     * @return      消息的顺序ID。
     */
    public Long getSequenceID() {
        return sequenceID;
    }

    /**
     * 读取消息实体。
     * @return  消息实体。
     */
    public IMessage getMessage(){
        return message;
    }
}

