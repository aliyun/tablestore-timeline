package com.alicloud.openservices.tablestore.timeline2.model;

/**
 * The meta of timeline,
 */
public class TimelineMeta extends DynamicRow<TimelineMeta> {
    private TimelineIdentifier identifier;

    public TimelineMeta(TimelineIdentifier identifier) {
        this.identifier = identifier;
    }

    /**
     * Get the identifier of this timeline meta.
     *
     * @return TimelineIdentifier
     */
    public TimelineIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public String toString() {
        return "Identifier: " + identifier + ", Columns: [" + super.toString() + "]";
    }
}
