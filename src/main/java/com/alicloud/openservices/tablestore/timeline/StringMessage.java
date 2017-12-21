package com.alicloud.openservices.tablestore.timeline;

/**
 * 一种简单的String类型的消息
 */
public class StringMessage extends DistinctMessage {
    private String content = null;
    private String messageID = null;

    /**
     * 字符串类型消息的构造函数。
     */
    public StringMessage() {
        content = "";
    }

    /**
     * 字符串类型消息的构造函数。
     * @param messageID 消息ID，需要保证同一个会话一段时间内唯一。
     * @param content   消息内容。
     */
    public StringMessage(String messageID, String content) {
        this.messageID = messageID;
        this.content = content;
    }

    /**
     * 字符串类型消息的构造函数。
     * @param content   消息内容。
     */
    public StringMessage(String content) {
        this.messageID = super.getMessageID();
        this.content = content;
    }

    /**
     * 返回消息内容
     * @return      消息内容
     */
    public String getContent() {
        return content;
    }

    @Override
    public IMessage newInstance() {
        return new StringMessage();
    }

    @Override
    public byte[] serialize()  {
        return content.getBytes();
    }

    @Override
    public void deserialize(byte[] input) {
        content = new String(input);
    }

    @Override
    public String getMessageID() {
        return messageID;
    }

    @Override
    public void setMessageID(String messageID) {
        this.messageID = messageID;
    }
}
