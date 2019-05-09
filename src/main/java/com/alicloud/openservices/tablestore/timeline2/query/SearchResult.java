package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;

import java.util.List;

public class SearchResult<T> {
    public static class Entry<T> {
        private TimelineIdentifier identifier;
        private T data;

        public Entry(TimelineIdentifier identifier, T data) {
            this.identifier = identifier;
            this.data = data;
        }

        public TimelineIdentifier getIdentifier() {
            return identifier;
        }

        public T getData() {
            return data;
        }
    }

    private List<Entry<T>> entries;
    private long totalCount;
    private boolean isAllSucceed;
    private byte[] nextToken;

    public SearchResult(List<Entry<T>> entries, boolean isAllSucceed, long totalCount, byte[] nextToken) {
        this.entries = entries;
        this.isAllSucceed = isAllSucceed;
        this.totalCount = totalCount;
        this.nextToken = nextToken;
    }

    public List<Entry<T>> getEntries() {
        return entries;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public boolean isAllSucceed() {
        return isAllSucceed;
    }

    public byte[] getNextToken() {
        return nextToken;
    }
}
