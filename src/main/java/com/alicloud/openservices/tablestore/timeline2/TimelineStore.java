package com.alicloud.openservices.tablestore.timeline2;

import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;

/**
 * The store service of timeline.
 */
public interface TimelineStore {

    /**
     * Create and get the timeline queue with specified identifier.
     *
     * @param  identifier         The identifier of timeline.
     *
     * @return TimelineQueue
     */
    TimelineQueue createTimelineQueue(TimelineIdentifier identifier);

    /**
     * Search timeline entries by search parameter.
     * Search will throw TimelineException when index info not set in TimelineSchema.
     *
     * @param searchParameter   The parameter of search, which will convert to SearchQuery.
     *
     * @return SearchResult<TimelineEntry>
     */
    SearchResult<TimelineEntry> search(SearchParameter searchParameter);

    /**
     * Search TimelineEntry by search parameter.
     * Search will throw TimelineException when index info not set in TimelineSchema.
     *
     * @param searchQuery   The SearchQuery of search, which is self-defined query condition.
     *
     * @return SearchResult<TimelineEntry>
     */
    SearchResult<TimelineEntry> search(SearchQuery searchQuery);

    /**
     * Create the table of timeline.
     * And create the SearchIndex of timeline if necessary.
     */
    void prepareTables();

    /**
     * Drop the table of timeline.
     * And drop the SearchIndex of timeline if exist.
     **/
    void dropAllTables();

    /**
     * Flush all the messages in buffer, wait until finish writing.
     */
    void flush();

    /**
     * Close the writer and thread pool.
     */
    void close();
}
