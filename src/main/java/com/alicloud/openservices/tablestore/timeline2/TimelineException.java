package com.alicloud.openservices.tablestore.timeline2;

public class TimelineException extends RuntimeException {
    public TimelineException() {
        super();
    }

    public TimelineException(String message) {
        super(message);
    }

    public TimelineException(Throwable cause) {
        super(cause);
    }

    public TimelineException(String message, Throwable cause) {
        super(message, cause);
    }
}
