package com.alicloud.openservices.tablestore.timeline2;

import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMeta;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;

/**
 * The store service of timeline meta.
 */
public interface TimelineMetaStore {
    /**
     * Get timeline meta by identifier.
     * Return null if this timeline meta is not exist.
     *
     * @param identifier        The identifier of timeline meta.
     *
     * @return TimelineMeta
     */
    TimelineMeta read(TimelineIdentifier identifier);

    /**
     * Search timeline meta by search parameter.
     * Search will throw TimelineException when index info not set in TimelineSchema.
     *
     * @param searchParameter   The parameter of search, which will convert to SearchQuery.
     *
     * @return SearchResult<TimelineMeta>
     */
    SearchResult<TimelineMeta> search(SearchParameter searchParameter);

    /**
     * Search timeline meta by search parameter.
     * Search will throw TimelineException when index info not set in TimelineSchema.
     *
     * @param searchQuery       The searchQuery of search, which is self-defined query condition.
     *
     * @return SearchResult<TimelineMeta>
     */
    SearchResult<TimelineMeta> search(SearchQuery searchQuery);

    /**
     * Insert a new timeline meta with properties.
     *
     * @param meta              The meta of timeline.
     *
     * @return TimelineMeta
     */
    TimelineMeta insert(TimelineMeta meta);

    /**
     * Update existed timeline meta with new properties.
     * It will insert a new meta if the timeline meta not exist.
     *
     * @param meta              the meta of timeline.
     *
     * @return TimelineMeta
     */
    TimelineMeta update(TimelineMeta meta);

    /**
     * Delete existed timeline meta by identifier.
     * It won't throw exception, when the timeline meta with this identifier not exist.
     *
     * @param identifier        The identifier of the timeline to be delete.
     */
    void delete(TimelineIdentifier identifier);

    /**
     * Create the table of meta store;
     * And create the SearchIndex of timeline meta if necessary.
     */
    void prepareTables();

    /**
     * Drop the table of meta store.
     * And Drop the SearchIndex of timeline meta if exist.
     **/
    void dropAllTables();

    /**
     * Close store service.
     */
    void close();
}
