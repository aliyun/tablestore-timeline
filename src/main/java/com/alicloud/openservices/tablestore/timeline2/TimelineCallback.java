package com.alicloud.openservices.tablestore.timeline2;


import com.alicloud.openservices.tablestore.timeline2.model.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMessage;

public interface TimelineCallback {
    /**
     * Function to invoke when succeed.
     * @param identifier        The identifier of timeline.
     * @param message           The message which is updated or stored asynchronously.
     * @param timelineEntry     The timeline entry.
     */
    void onCompleted(final TimelineIdentifier identifier, final TimelineMessage message, final TimelineEntry timelineEntry);

    /**
     * Function to invoke when failed.
     * @param identifier        The identifier of timeline.
     * @param message           The message which is updated or stored asynchronously.
     * @param ex                The exception when failed.
     */
    void onFailed(final TimelineIdentifier identifier, final TimelineMessage message, final Exception ex);
}
