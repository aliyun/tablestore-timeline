package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline2.common.Conf;
import com.alicloud.openservices.tablestore.timeline2.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Arrays;

import static com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper.sleepForSyncData;
import static org.junit.Assert.assertEquals;

public class TestSearchQuery {
    private static String metaTableName = "metaTable";
    private static String metaIndexName = "metaIndex";
    private static String dataTableName = "dataTable";
    private static String dataIndexName = "dataIndex";
    private static String sequenceIdName = "dataIndex";


    private static TimelineIdentifierSchema identifierSchema = new TimelineIdentifierSchema.Builder()
            .addStringField("timelineId")
            .build();

    private static SyncClient client;
    private static TimelineStoreFactory factory = null;
    private static TimelineMetaStore metaService = null;
    private static TimelineStore timelineStore = null;

    @BeforeClass
    public static void setUp() throws Exception {
        try {
            Conf conf = Conf.newInstance(System.getProperty("user.home") + "/timelineConf.json");
            client = new SyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstanceName());
            factory = new TimelineStoreFactoryImpl(client);

            initMetaTable();
            initDataTable();

            insertMeta();
            insertData();

            sleepForSyncData();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void after() {
        timelineStore.flush();

        metaService.dropAllTables();
        timelineStore.dropAllTables();

        client.shutdown();
    }

    @Test
    public void testMatchSearchMeta() {
        MatchQuery query = new MatchQuery();
        query.setFieldName("groupName");
        query.setText("表格");

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testMatchPhraseSearchMeta() {
        MatchQuery query = new MatchQuery();
        query.setFieldName("groupName");
        query.setText("存储");

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);


        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testRangeSearchMeta() {
        RangeQuery query = new RangeQuery();
        query.setFieldName("point");
        query.setTo(ColumnValue.fromDouble(4), true);

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(5, result.getTotalCount());
    }

    @Test
    public void testBooleanSearchMeta() {
        TermQuery query = new TermQuery();
        query.setFieldName("isPublic");
        query.setTerm(ColumnValue.fromBoolean(true));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(5, result.getTotalCount());
    }

    @Test
    public void testTermSearchMeta() {
        TermQuery query = new TermQuery();
        query.setFieldName("tags");
        query.setTerm(ColumnValue.fromString("Table"));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
    }

    @Test
    public void testTermsSearchMeta() {
        TermsQuery query = new TermsQuery();
        query.setFieldName("tags");
        query.setTerms(Arrays.asList(ColumnValue.fromString("Table"), ColumnValue.fromString("Store")));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
    }

    @Test
    public void testWildcardSearchMeta() {
        WildcardQuery query = new WildcardQuery();
        query.setFieldName("tags");
        query.setValue("Tab*");

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
    }

    @Test
    public void testPrefixSearchMeta() {
        PrefixQuery query = new PrefixQuery();
        query.setFieldName("tags");
        query.setPrefix("Sto");

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(2);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
    }

    @Test
    public void testAndSearchMeta() {
        TermQuery query1 = new TermQuery();
        query1.setFieldName("groupName");
        query1.setTerm(ColumnValue.fromString("表格"));

        RangeQuery query2 = new RangeQuery();
        query2.setFieldName("createTime");
        query2.setFrom(ColumnValue.fromLong(0), true);

        TermQuery query3 = new TermQuery();
        query3.setFieldName("isPublic");
        query3.setTerm(ColumnValue.fromBoolean(true));

        RangeQuery query4 = new RangeQuery();
        query4.setFieldName("point");
        query4.setTo(ColumnValue.fromDouble(10), true);

        TermsQuery query5 = new TermsQuery();
        query5.setFieldName("tags");
        query5.setTerms(Arrays.asList(ColumnValue.fromString("Table"), ColumnValue.fromString("Store")));

        GeoDistanceQuery query6 = new GeoDistanceQuery();
        query6.setFieldName("location");
        query6.setCenterPoint("30,121");
        query6.setDistanceInMeter(500000);

        BoolQuery query = new BoolQuery();
        query.setMustQueries(Arrays.asList(query1, query2, query3, query4, query5, query6));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(5);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(4, result.getTotalCount());
    }

    @Test
    public void testOrSearchMeta() {
        TermQuery query1 = new TermQuery();
        query1.setFieldName("groupName");
        query1.setTerm(ColumnValue.fromString("表格"));

        RangeQuery query2 = new RangeQuery();
        query2.setFieldName("createTime");
        query2.setFrom(ColumnValue.fromLong(0), true);

        TermQuery query3 = new TermQuery();
        query3.setFieldName("isPublic");
        query3.setTerm(ColumnValue.fromBoolean(true));

        RangeQuery query4 = new RangeQuery();
        query4.setFieldName("point");
        query4.setTo(ColumnValue.fromDouble(10), true);

        TermsQuery query5 = new TermsQuery();
        query5.setFieldName("tags");
        query5.setTerms(Arrays.asList(ColumnValue.fromString("Table"), ColumnValue.fromString("Store")));

        BoolQuery query = new BoolQuery();
        query.setShouldQueries(Arrays.asList(query1, query2, query3, query4, query5));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(5);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(10, result.getTotalCount());
    }

    @Test
    public void testNotSearchMeta() {
        RangeQuery query2 = new RangeQuery();
        query2.setFieldName("createTime");
        query2.setTo(ColumnValue.fromLong(6), true);

        TermQuery query3 = new TermQuery();
        query3.setFieldName("isPublic");
        query3.setTerm(ColumnValue.fromBoolean(true));

        RangeQuery query4 = new RangeQuery();
        query4.setFieldName("point");
        query4.setTo(ColumnValue.fromDouble(6), true);

        GeoDistanceQuery query5 = new GeoDistanceQuery();
        query5.setFieldName("location");
        query5.setCenterPoint("30,121");
        query5.setDistanceInMeter(100);

        BoolQuery query = new BoolQuery();
        query.setMustNotQueries(Arrays.asList(query2, query3, query4));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(5);

        SearchResult<TimelineMeta> result = metaService.search(searchQuery);
        assertEquals(2, result.getTotalCount());
    }


    @Test
    public void testAndSearchTimeline() {
        TermQuery query1 = new TermQuery();
        query1.setFieldName("timelineId");
        query1.setTerm(ColumnValue.fromString("group_1"));

        MatchQuery query2 = new MatchQuery();
        query2.setFieldName("text");
        query2.setText("hello");

        RangeQuery query3 = new RangeQuery();
        query3.setFieldName("timestamp");
        query3.setTo(ColumnValue.fromDouble(2), true);

        TermQuery query4 = new TermQuery();
        query4.setFieldName("from");
        query4.setTerm(ColumnValue.fromString("ots"));

        TermsQuery query5 = new TermsQuery();
        query5.setFieldName("receivers");
        query5.setTerms(Arrays.asList(ColumnValue.fromString("user_a"), ColumnValue.fromString("user_b")));

        BoolQuery query = new BoolQuery();
        query.setMustQueries(Arrays.asList(query1, query2, query3, query4, query5));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(5);

        SearchResult<TimelineEntry> result = timelineStore.search(searchQuery);
        assertEquals(3, result.getTotalCount());
    }

    @Test
    public void testOrSearchTimeline() {
        TermQuery query1 = new TermQuery();
        query1.setFieldName("timelineId");
        query1.setTerm(ColumnValue.fromString("group_1"));

        MatchQuery query2 = new MatchQuery();
        query2.setFieldName("text");
        query2.setText("hello");

        RangeQuery query3 = new RangeQuery();
        query3.setFieldName("timestamp");
        query3.setTo(ColumnValue.fromDouble(2), true);

        TermQuery query4 = new TermQuery();
        query4.setFieldName("from");
        query4.setTerm(ColumnValue.fromString("ots"));

        TermsQuery query5 = new TermsQuery();
        query5.setFieldName("receivers");
        query5.setTerms(Arrays.asList(ColumnValue.fromString("user_a"), ColumnValue.fromString("user_b")));

        BoolQuery query = new BoolQuery();
        query.setShouldQueries(Arrays.asList(query1, query2, query3, query4, query5));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(5);

        SearchResult<TimelineEntry> result = timelineStore.search(searchQuery);
        assertEquals(20, result.getTotalCount());
    }

    @Test
    public void testNotSearchTimeline() {
        TermQuery query1 = new TermQuery();
        query1.setFieldName("timelineId");
        query1.setTerm(ColumnValue.fromString("group_3"));

        MatchQuery query2 = new MatchQuery();
        query2.setFieldName("text");
        query2.setText("hello");

        RangeQuery query3 = new RangeQuery();
        query3.setFieldName("timestamp");
        query3.setTo(ColumnValue.fromDouble(2), true);

        TermQuery query4 = new TermQuery();
        query4.setFieldName("from");
        query4.setTerm(ColumnValue.fromString("ots123"));

        BoolQuery query = new BoolQuery();
        query.setMustNotQueries(Arrays.asList(query1, query2, query3, query4));

        SearchQuery searchQuery = new SearchQuery();
        searchQuery.setQuery(query);
        searchQuery.setGetTotalCount(true);
        searchQuery.setLimit(5);

        SearchResult<TimelineEntry> result = timelineStore.search(searchQuery);
        assertEquals(7, result.getTotalCount());
    }

    /**
     * init meta table service
     * */
    private static void initMetaTable() {
        IndexSchema metaIndex = new IndexSchema();
        metaIndex.addFieldSchema(new FieldSchema("groupName", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        metaIndex.addFieldSchema(new FieldSchema("createTime", FieldType.LONG).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("location", FieldType.GEO_POINT).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("isPublic", FieldType.BOOLEAN).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("point", FieldType.DOUBLE).setIndex(true));
        metaIndex.addFieldSchema(new FieldSchema("tags", FieldType.KEYWORD).setIndex(true).setIsArray(true));

        TimelineMetaSchema metaSchema = new TimelineMetaSchema(metaTableName, identifierSchema)
                .withIndex(metaIndexName, metaIndex);
        metaService = factory.createMetaStore(metaSchema);
        metaService.prepareTables();
    }

    /**
     * init store table service
     * */
    private static void initDataTable() {
        IndexSchema dataIndex = new IndexSchema();
        dataIndex.addFieldSchema(new FieldSchema("timelineId", FieldType.KEYWORD).setIndex(true));
        dataIndex.addFieldSchema(new FieldSchema("text", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        dataIndex.addFieldSchema(new FieldSchema("receivers", FieldType.KEYWORD).setIsArray(true).setIndex(true));
        dataIndex.addFieldSchema(new FieldSchema("timestamp", FieldType.LONG).setEnableSortAndAgg(true));
        dataIndex.addFieldSchema(new FieldSchema("from", FieldType.KEYWORD));

        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1000);

        TimelineSchema dataSchema = new TimelineSchema(dataTableName, identifierSchema)
                .withIndex(dataIndexName, dataIndex)
                .autoGenerateSeqId()
                .setSequenceIdColumnName(sequenceIdName)
                .setTimeToLive(-1)
                .withWriterConfig(writerConfig);
        timelineStore = factory.createTimelineStore(dataSchema);
        timelineStore.prepareTables();
    }

    /**
     * insert meta into metaTable
     * */
    private static void insertMeta() {
        for (int i = 0; i < 10; i++) {
            TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                    .addField("timelineId", "group_" + i)
                    .build();

            TimelineMeta insertGroup = new TimelineMeta(identifier)
                    .setField("groupName", "表格存储" + i)
                    .setField("createTime", i)
                    .setField("location", "30,12" + i)
                    .setField("isPublic", i % 2 == 0)
                    .setField("point", i + 0.0D)
                    .setField(new Column("tags", ColumnValue.fromString("[\"Table\",\"Store\"]")));

            metaService.insert(insertGroup);
        }
    }

    /**
     * insert data into dataTable
     * */
    private static void insertData() {
        String[] groupMember = new String[]{"user_a", "user_b", "user_c"};


        for (int i = 0; i < 10; i++) {
            TimelineQueue groupTimelineQueue = timelineStore.createTimelineQueue(
                    new TimelineIdentifier.Builder()
                            .addField("timelineId", "group_1")
                            .build()
            );

            TimelineMessage tm = new TimelineMessage()
                    .setField("text", "hello TableStore ots" + i)
                    .setField("receivers", groupMember)
                    .setField("timestamp", i)
                    .setField("from", "ots");
            groupTimelineQueue.store(tm);
        }

        for (int i = 0; i < 10; i++) {
            TimelineQueue groupTimelineQueue = timelineStore.createTimelineQueue(
                    new TimelineIdentifier.Builder()
                            .addField("timelineId", "group_2")
                            .build()
            );

            TimelineMessage tm = new TimelineMessage()
                    .setField("text", "fine TableStore ots" + i)
                    .setField("receivers", groupMember)
                    .setField("timestamp", i)
                    .setField("from", "ots");
            groupTimelineQueue.store(tm);
        }
    }
}
