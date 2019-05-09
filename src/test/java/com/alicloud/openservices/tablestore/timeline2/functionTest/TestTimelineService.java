package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.RangeQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;
import org.junit.*;

import java.util.Arrays;

import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.*;
import static org.junit.Assert.*;

public class TestTimelineService {
    private static ServiceWrapper wrapper = null;

    @BeforeClass
    public static void setUp() throws Exception {
        wrapper = ServiceWrapper.newInstance();

        TimelineMetaStore metaService = wrapper.getMetaStore();
        TimelineStore storeTableService = wrapper.getStoreTableStore();

        metaService.prepareTables();
        storeTableService.prepareTables();

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();

        TimelineMeta insertGroup = new TimelineMeta(identifier)
                .setField("groupName", "tablestore")
                .setField("createTime", 1);

        metaService.insert(insertGroup);


        TimelineQueue groupStore =  storeTableService.createTimelineQueue(identifier);
        for (int i = 0; i < 10; i++) {
            TimelineMessage msg = generateMessage(i);
            groupStore.store(msg);
        }

        /**
         * wait for sync data into searchIndex
         * */
        ServiceWrapper.sleepForSyncData();
    }

    @AfterClass
    public static void after() throws Exception {
        wrapper.getMetaStore().dropAllTables();
        wrapper.getStoreTableStore().dropAllTables();

        wrapper.shutdown();
    }

    @Test
    public void testSearchBySearchParameter() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();
        TimelineStore storeService =  wrapper.getStoreTableStore();


        SearchParameter parameter = new SearchParameter(
                and(
                        field("text").equals("hello"),
                        field("timestamp").greaterEqual(0),
                        field("timestamp").lessEqual(5),
                        field("receivers").equals("user_a")
                )
        ).limit(1).offset(1).calculateTotalCount().orderBy(new String[]{"timestamp"}, SortOrder.ASC);

        SearchResult<TimelineEntry> searchResult = storeService.search(parameter);

        assertTrue(searchResult.isAllSucceed());
        assertEquals(6, searchResult.getTotalCount());
        assertNotNull(searchResult.getNextToken());
        assertEquals(1, searchResult.getEntries().size());


        TimelineEntry entry = searchResult.getEntries().get(0).getData();

        assertEquals("hello tablestore1", entry.getMessage().getString("text"));
        assertEquals(1, entry.getMessage().getLong("timestamp"));


        parameter.withToken(searchResult.getNextToken());
        searchResult = storeService.search(parameter);

        assertTrue(identifier.equals(searchResult.getEntries().get(0).getIdentifier()));
    }

    @Test
    public void testSearchBySearchQuery() {
        TimelineStore storeService =  wrapper.getStoreTableStore();

        TermQuery termQuery = new TermQuery();
        termQuery.setFieldName("receivers");
        termQuery.setTerm(ColumnValue.fromString("user_a"));

        RangeQuery rangeQuery = new RangeQuery();
        rangeQuery.setFieldName("timestamp");
        rangeQuery.greaterThanOrEqual(ColumnValue.fromLong(5));
        rangeQuery.lessThanOrEqual(ColumnValue.fromLong(6));

        BoolQuery boolQuery = new BoolQuery();
        boolQuery.setMustQueries(Arrays.asList(termQuery, rangeQuery));

        SearchQuery searchQuery = new SearchQuery().setQuery(boolQuery);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(1);
        searchQuery.setOffset(1);

        Sort.Sorter fieldSort = new FieldSort("timestamp", SortOrder.DESC);
        searchQuery.setSort(new Sort(Arrays.asList(fieldSort)));

        SearchResult<TimelineEntry> searchResult1 = storeService.search(searchQuery);

        assertTrue(searchResult1.isAllSucceed());
        assertEquals(2, searchResult1.getTotalCount());
        assertEquals(1, searchResult1.getEntries().size());

        TimelineEntry entry1 = searchResult1.getEntries().get(0).getData();

        assertEquals("hello tablestore5", entry1.getMessage().getString("text"));
        assertEquals(5, entry1.getMessage().getLong("timestamp"));
    }

    @Test
    public void testExceptionSearch() {
        TimelineStore storeService = wrapper.getStoreTableStore();
        TimelineStore syncService = wrapper.getSyncTableStore();

        /**
         * Test search exception
         * */
        TermQuery wrongQuery = new TermQuery();
        wrongQuery.setFieldName("notExist");
        wrongQuery.setTerm(ColumnValue.fromLong(8));

        SearchQuery wrongSearchQuery = new SearchQuery().setQuery(wrongQuery);
        wrongSearchQuery.setGetTotalCount(true);
        wrongSearchQuery.setLimit(1);

        try {
            storeService.search(wrongSearchQuery);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }

        try {
            syncService.search(wrongSearchQuery);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("The store not support search cause not has data index", e.getMessage());
        }
    }

    @Test
    public void testCreateTimelineWithNull() {
        TimelineStore storeService = wrapper.getStoreTableStore();
        try {
            storeService.createTimelineQueue(null);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("Identifier should not be null.", e.getMessage());
        }
    }

    private static TimelineMessage generateMessage(int i) {
        return new TimelineMessage()
                .setField("text", "hello tablestore" + i)
                .setField("receivers", Arrays.asList("user_a", "user_b"))
                .setField("timestamp", i);
    }

}

