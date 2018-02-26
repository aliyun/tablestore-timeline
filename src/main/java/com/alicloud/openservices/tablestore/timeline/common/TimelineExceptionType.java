package com.alicloud.openservices.tablestore.timeline.common;

/**
 * Timeline异常的类型。
 */
public enum TimelineExceptionType {
    /**
     * 可重试。
     */
    RETRY,

    /**
     * 使用方式有误。
     */
    INVALID_USE,

    /**
     * 不应该出现的异常出现，建议退出检查。
     */
    ABORT,

    /**
     * 未知异常。
     */
    UNKNOWN
}
