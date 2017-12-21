package com.alicloud.openservices.tablestore.timeline;

public class TimelineException extends RuntimeException {
    private TimelineExceptionType type = null;

    public TimelineException(TimelineExceptionType type, String message) {
        super(message);
        this.type = type;
    }

    public TimelineException(TimelineExceptionType type, String message, Throwable cause) {
        super(message, cause);
        this.type = type;
    }

    TimelineExceptionType getType() {
        return this.type;
    }
}
