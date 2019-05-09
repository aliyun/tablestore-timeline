package com.alicloud.openservices.tablestore.timeline2.core;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMetaSchema;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineSchema;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;

/**
 * The factory which provides the store service of timeline meta and timeline.
 * Timeline V2 based on TableStore with SearchIndex.
 */
public class TimelineStoreFactoryImpl implements TimelineStoreFactory {
    private SyncClient client;

    public TimelineStoreFactoryImpl(SyncClient client) {
        Preconditions.checkNotNull(client, "SyncClient should not be null.");

        this.client = client;
    }

    /**
     * Create and get timeline store to menage timeline.
     * @param timelineSchema    The schema of timeline, include table name, primary key, index schema and etc.
     */
    @Override
    public TimelineStore createTimelineStore(TimelineSchema timelineSchema) {
        Preconditions.checkNotNull(timelineSchema, "TimelineSchema should not be null.");

        return new TimelineStoreImpl(client, timelineSchema);
    }

    /**
     * Create and get timeline meta service to menage metas.
     * @param metaSchema       The schema of timeline meta, include table name, primary key, index schema and etc.
     */
    @Override
    public TimelineMetaStore createMetaStore(TimelineMetaSchema metaSchema) {
        Preconditions.checkNotNull(metaSchema, "TimelineMetaSchema should not be null.");

        return new TimelineMetaStoreImpl(client, metaSchema);
    }
}
