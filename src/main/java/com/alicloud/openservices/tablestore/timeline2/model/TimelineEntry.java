package com.alicloud.openservices.tablestore.timeline2.model;

/**
 * Entry of timeline, each entry contains message along with a distinct sequence id.
 */
public class TimelineEntry {
    private long sequenceID;
    private TimelineMessage message;

    public TimelineEntry(long sequenceID, TimelineMessage message) {
        this.sequenceID = sequenceID;
        this.message = message;
    }

    /**
     * Get the sequence id of this entry in timeline.
     *
     * @return the sequence id
     */
    public long getSequenceID() {
        return sequenceID;
    }

    /**
     * Get the message of this entry in timeline.
     *
     * @return the message
     */
    public TimelineMessage getMessage(){
        return message;
    }
}
