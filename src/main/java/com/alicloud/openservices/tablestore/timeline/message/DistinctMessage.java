package com.alicloud.openservices.tablestore.timeline.message;

import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.common.TimelineExceptionType;
import com.alicloud.openservices.tablestore.timeline.utils.Utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息接口的一种抽象实现，此抽象类会自动创建消息ID。
 * 如果继承DistinctMessage实现自己的消息类，则在自定义消息类中不需要再次实现getMessageID接口。
 */
public abstract class DistinctMessage implements IMessage {
    private static AtomicInteger baseID = new AtomicInteger(0);
    private static String machineID = Utils.getProcessID() + "@" + Utils.getLocalIP() + ":";
    private String messageID = null;

    /**
     * 这里的整数（Integer）范围选择了(0, Integer.MAX_VALUE)，会按递增顺序循环使用这个范围的值。
     * 这里的MessageID是machineID + 递增ID（每个进程内循环递增的，不同机器，不同进程的machineID不同）。
     * 对于Timeline模型，消息ID只需要在当前会话中唯一即可。
     * 比如在IM中，只需要在某个会话或者群里面唯一即可，这时候其实更好的方式是由客户端生成这个消息ID。
     * 如果是客户端生成消息ID，则同一个会话的消息可以同时发往多个进程处理，不再有必须同一个进程处理的限制。
     * @return  消息ID。
     */
    @Override
    public String getMessageID() {
        if (messageID == null) {
            baseID.compareAndSet(Integer.MAX_VALUE, 0);
            messageID = machineID + String.valueOf(baseID.addAndGet(1));
        }
        return String.valueOf(messageID);
    }

    @Override
    public void setMessageID(String messageID) {
        if (messageID == null || messageID.isEmpty()) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE, "message id is null or empty.");
        }
        this.messageID = messageID;
    }
}
