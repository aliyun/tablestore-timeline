package com.alicloud.openservices.tablestore.timeline2;

import com.alicloud.openservices.tablestore.timeline2.model.TimelineMetaSchema;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineSchema;

/**
 * The factory which provides the service of timeline meta and timeline.
 */
public interface TimelineStoreFactory {

    /**
     * Create timeline store service with timeline schema.
     *
     * @param timelineSchema    The schema of timeline, include table name, primary key, index schema and etc.
     *
     * @return TimelineStore
     */
    TimelineStore createTimelineStore(TimelineSchema timelineSchema);

    /**
     * Create timeline meta store service with meta schema.
     *
     * @param metaSchema        The schema of timeline meta, include table name, primary key, index schema and etc.
     *
     * @return TimelineMetaStore
     */
    TimelineMetaStore createMetaStore(TimelineMetaSchema metaSchema);
}
