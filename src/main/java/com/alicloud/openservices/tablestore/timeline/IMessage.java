package com.alicloud.openservices.tablestore.timeline;

/**
 * 消息类的接口，LIB的使用者需要继承IMessage或者DistinctMessage实现自己的Message类。
 */
public interface IMessage {
    /**
     * 消息ID，用户实现的消息雷必须实现此接口。
     * 如果为null，LIB不用自动生成message id。如果要用自动生成，要继承DistinctMessage。
     * 消息ID的作用主要是用于消息去重，最大使用场景是IM中的多人群。
     * 对于Timeline模型，消息ID只需要在当前会话中唯一即可。
     * 比如在IM中，只需要在某个会话或者群里面唯一即可，这时候其实更好的方式是由客户端生成这个消息ID。
     * 最简单的方式是客户端循环使用0~10000之间的值作为ID即可满足要求。
     * @return  消息ID
     */
    String getMessageID();

    /**
     * 设置消息ID。用于读取到消息时填充用。
     * @param messageID     消息ID。
     */
    void setMessageID(String messageID);

    /**
     * 生成一个同类型对象。
     * @return  同类型对象。
     */
    IMessage newInstance();

    /**
     * 序列化消息体。
     * @return 序列化后的字节。
     */
    byte[] serialize();

    /**
     * 反序列化字节流为消息对象。
     * @param input  待反序列化的消息对象的字节。
     */
    void deserialize(byte[] input);
}
