package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;

public class ScanParameter {
    private long from = 0;
    private long to = Long.MAX_VALUE;
    private boolean isForward = true;

    private int maxCount = 100;
    private Filter filter;

    public ScanParameter() {}

    public ScanParameter scanForward(long from, long to) {
        this.from = from;
        this.to = to;
        this.isForward = true;
        checkCondition();
        return this;
    }

    public ScanParameter scanForward(long from) {
        return scanForward(from, Long.MAX_VALUE);
    }

    public ScanParameter scanForwardTo(long to) {
        return scanForward(0, to);
    }

    public ScanParameter scanBackward(long from, long to) {
        this.from = from;
        this.to = to;
        this.isForward = false;
        checkCondition();
        return this;
    }

    public ScanParameter scanBackward(long from) {
        return scanBackward(from, 0);
    }

    public ScanParameter scanBackwardTo(long to) {
        return scanBackward(Long.MAX_VALUE, to);
    }

    private void checkCondition() {
        boolean isValid = true;
        if (isForward) {
            isValid = from >= 0 && from < to;
        } else {
            isValid = to >= 0 && to < from;
        }

        if (!isValid) {
            Preconditions.checkArgument(
                    isValid,
                    "Invalid scan parameter with forward set to '" + isForward + "' from " + from + " to " + to + "."
            );
        }
    }

    public ScanParameter maxCount(int maxCount) {
        this.maxCount = maxCount;
        return this;
    }

    public ScanParameter withFilter(Filter filter) {
        this.filter = filter;
        return this;
    }

    public long getFrom() {
        return from;
    }

    public long getTo() {
        return to;
    }

    public boolean isForward() {
        return isForward;
    }

    public int getMaxCount() {
        return maxCount;
    }

    public Filter getFilter() {
        return filter;
    }
}
