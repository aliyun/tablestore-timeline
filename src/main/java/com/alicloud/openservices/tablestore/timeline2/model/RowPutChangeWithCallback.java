package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.timeline2.TimelineCallback;

import java.util.concurrent.Future;

public class RowPutChangeWithCallback extends RowPutChange {
    private TimelineIdentifier identifier = null;
    private TimelineCallbackImpledFuture<TimelineMessage, TimelineEntry> future = new TimelineCallbackImpledFuture<TimelineMessage, TimelineEntry>();

    public RowPutChangeWithCallback(String tableName, PrimaryKey primaryKey) {
        super(tableName, primaryKey);
    }

    public void setComplete(TimelineEntry timelineEntry) {
        TimelineMessage message = new TimelineMessage();
        message.setFields(this.getColumnsToPut());

        future.onCompleted(message, timelineEntry);
    }

    public void setFailed(Exception ex) {
        TimelineMessage message = new TimelineMessage();
        message.setFields(this.getColumnsToPut());

        future.onFailed(message, ex);
    }

    public RowPutChangeWithCallback watchBy(final TimelineCallback callback) {
        final TimelineMessage message = new TimelineMessage();
        message.setFields(this.getColumnsToPut());
        TableStoreCallback<TimelineMessage, TimelineEntry> tsCallback = new TableStoreCallback<TimelineMessage, TimelineEntry>() {

            @Override
            public void onCompleted(TimelineMessage message, TimelineEntry timelineEntry) {
                callback.onCompleted(identifier, message, timelineEntry);
            }

            @Override
            public void onFailed(TimelineMessage message, Exception e) {
                callback.onFailed(identifier, message, e);
            }
        };

        future.watchBy(tsCallback);
        return this;
    }

    public RowPutChangeWithCallback withTimelineIdentifier(TimelineIdentifier identifier) {
        this.identifier = identifier;
        return this;
    }

    public Future<TimelineEntry> getFuture() {
        return future;
    }
}
