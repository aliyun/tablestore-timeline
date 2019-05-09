package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;
import org.junit.*;

import java.util.Arrays;

import static com.alicloud.openservices.tablestore.timeline2.common.Cons.LONG_INVALID_COLUME_NAME;
import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.*;


import static org.junit.Assert.*;

public class TestMetaService {
    private static ServiceWrapper wrapper = null;

    @BeforeClass
    public static void setUp() throws Exception {
        wrapper = ServiceWrapper.newInstance();
        wrapper.getMetaStore().prepareTables();
    }

    @AfterClass
    public static void after() throws Exception {
        wrapper.getMetaStore().dropAllTables();
        wrapper.shutdown();
    }

    @Test
    public void testInsertMeta() {
        TimelineMetaStore metaService = wrapper.getMetaStore();

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();

        TimelineMeta insertGroup = new TimelineMeta(identifier)
                .setField("Long", 1000L)
                .setField("double", 1.1)
                .setField("groupName", "tablestore")
                .setField("boolean", true)
                .setField(new Column("stringList", ColumnValue.fromString("[\"stringList\"]")));

        metaService.insert(insertGroup);
        TimelineMeta readGroup = metaService.read(identifier);

        assertNotNull(readGroup);

        assertTrue(identifier.equals(readGroup.getIdentifier()));
        assertEquals(1.1, readGroup.getDouble("double"), 0.001);
        assertEquals("tablestore", readGroup.getString("groupName"));
        assertEquals(1000L, readGroup.getLong("Long"));
        assertEquals(true, readGroup.getBoolean("boolean"));
        assertEquals(1, readGroup.getStringList("stringList").size());
        assertEquals("stringList", readGroup.getStringList("stringList").get(0));

        metaService.delete(identifier);

        try {
            insertGroup.setField(LONG_INVALID_COLUME_NAME, "invalid");
            metaService.insert(insertGroup);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testUpdateMeta() {
        TimelineMetaStore metaService = wrapper.getMetaStore();

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_a")
                .addField("long", 1000L)
                .build();

        TimelineMeta insertGroup = new TimelineMeta(identifier)
                .setField("groupName", "tablestore");

        metaService.insert(insertGroup);

        TimelineMeta updateGroup = insertGroup;
        updateGroup.setField("groupName", "timeline");
        metaService.update(updateGroup);

        TimelineMeta readGroup = metaService.read(identifier);

        assertNotNull(readGroup);
        assertTrue(identifier.equals(readGroup.getIdentifier()));
        assertEquals(identifier.hashCode(), readGroup.getIdentifier().hashCode());
        assertEquals("timeline", readGroup.getString("groupName"));
        assertEquals("Identifier: ['timelineId':group_a, 'long':1000], Columns: [groupName:timeline]", readGroup.toString());

        metaService.delete(identifier);

        /**
         * test update when meta not exit
         * */
        metaService.update(updateGroup);
        readGroup = metaService.read(identifier);
        assertNotNull(readGroup);
        metaService.delete(identifier);

        try {
            updateGroup.setField(LONG_INVALID_COLUME_NAME, "invalid");
            metaService.update(updateGroup);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testSearchMeta() {
        TimelineMetaStore metaService = wrapper.getMetaStore();

        /**
         * create 10 groupMeta
         * */
        for (long i = 0; i < 10; i++) {
            metaService.insert(
                    new TimelineMeta(
                            new TimelineIdentifier.Builder()
                                    .addField("timelineId", "group_" + i)
                                    .addField("long", 1000L)
                                    .build()
                    ).setField("groupName", "tablestore timeline" + i)
                    .setField("createTime", i)
            );
        }

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_8")
                .addField("long", 1000L)
                .build();


        /**
         * wait for sync data into searchIndex
         * */
        ServiceWrapper.sleepForSyncData();


        /**
         * test search by SearchParameter
         * */
        SearchParameter parameter = new SearchParameter(
                field("groupName").equals("tablestore")
        ).calculateTotalCount().limit(1).offset(7).orderBy(new String[]{"createTime"}, SortOrder.ASC);
        SearchResult<TimelineMeta> searchResult = metaService.search(parameter);
        assertEquals(10, searchResult.getTotalCount());
        assertTrue(searchResult.isAllSucceed());
        assertNotNull(searchResult.getNextToken());

        parameter.withToken(searchResult.getNextToken());
        searchResult = metaService.search(parameter);

        TimelineIdentifier searchIdentifier = searchResult.getEntries().get(0).getIdentifier();
        TimelineMeta searchGroup = searchResult.getEntries().get(0).getData();

        assertTrue(identifier.equals(searchIdentifier));
        assertEquals(8, searchGroup.getLong("createTime"));

        /**
         * test search by SearchQuery
         * */
        TermQuery query = new TermQuery();
        query.setFieldName("createTime");
        query.setTerm(ColumnValue.fromLong(8));

        SearchQuery searchQuery = new SearchQuery().setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(1);

        Sort.Sorter fieldSort = new FieldSort("createTime", SortOrder.DESC);
        searchQuery.setSort(new Sort(Arrays.asList(fieldSort)));

        searchResult = metaService.search(searchQuery);
        assertTrue(searchResult.isAllSucceed());
        assertNotNull(searchResult.getNextToken());
        assertEquals(1, searchResult.getTotalCount());

        searchIdentifier = searchResult.getEntries().get(0).getIdentifier();
        searchGroup = searchResult.getEntries().get(0).getData();

        assertTrue(identifier.equals(searchIdentifier));
        assertEquals(8, searchGroup.getLong("createTime"));

        metaService.delete(identifier);

    }

    @Test
    public void testExceptionSearch() {
        TimelineMetaStore metaService = wrapper.getMetaStore();

        /**
         * test search exception
         * */
        TermQuery wrongQuery = new TermQuery();
        wrongQuery.setFieldName("notExist");
        wrongQuery.setTerm(ColumnValue.fromLong(8));

        SearchQuery wrongSearchQuery = new SearchQuery().setQuery(wrongQuery);
        wrongSearchQuery.setGetTotalCount(true);
        wrongSearchQuery.setLimit(1);

        try {
            metaService.search(wrongSearchQuery);
        } catch (Exception e) {
            assertTrue(e instanceof TimelineException);
            assertEquals("OTSParameterInvalid", e.getMessage());
        }
    }

    @Test
    public void testDeleteMeta() {
        TimelineMetaStore metaService = wrapper.getMetaStore();

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timelineId", "group_delete")
                .addField("long", 1000L)
                .build();

        TimelineMeta insertGroup = new TimelineMeta(identifier)
                .setField("Long", 1000L)
                .setField("double", 1.1)
                .setField("groupName", "tablestore")
                .setField("boolean", true)
                .setField(new Column("stringList", ColumnValue.fromString("[\"stringList\"]")));

        metaService.insert(insertGroup);

        TimelineMeta readGroup = metaService.read(identifier);
        assertNotNull(readGroup);

        metaService.delete(identifier);
        readGroup = metaService.read(identifier);
        assertNull(readGroup);

        try {
            metaService.delete(identifier);
        } catch (Exception e) {
            fail();
        }
    }
}
