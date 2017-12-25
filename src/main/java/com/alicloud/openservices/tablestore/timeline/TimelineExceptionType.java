package com.alicloud.openservices.tablestore.timeline;

/**
 * Timeline异常的类型。
 */
public enum TimelineExceptionType {
    /**
     * 可重试。
     */
    TET_RETRY,

    /**
     * 使用方式有误。
     */
    TET_INVALID_USE,

    /**
     * 不应该出现的异常出现，建议退出检查。
     */
    TET_ABORT,

    /**
     * 未知异常。
     */
    TET_UNKNOWN
}
