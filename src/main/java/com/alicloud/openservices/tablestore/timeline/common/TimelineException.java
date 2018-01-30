package com.alicloud.openservices.tablestore.timeline.common;

/**
 * Timeline异常。
 */
public class TimelineException extends RuntimeException {
    private TimelineExceptionType type = null;

    /**
     * 构造函数。
     * @param type      异常类型，用于判断该异常的后续操作方式。
     * @param message   错误消息内容。
     */
    public TimelineException(TimelineExceptionType type, String message) {
        super(message);
        this.type = type;
    }

    /**
     * 构造函数。
     * @param type      异常类型，用于判断该异常的后续操作方式。
     * @param message   错误消息内容。
     * @param cause     引起此异常的上一个异常。
     */
    public TimelineException(TimelineExceptionType type, String message, Throwable cause) {
        super(message, cause);
        this.type = type;
    }

    /**
     * 获取异常类型。
     * @return  异常类型，
     */
    public TimelineExceptionType getType() {
        return this.type;
    }
}
