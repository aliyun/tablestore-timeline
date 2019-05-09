package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline2.common.Conf;
import com.alicloud.openservices.tablestore.timeline2.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;

import static com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper.sleepForSyncData;
import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestSearchParameter {
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
        SearchParameter parameter = new SearchParameter(
                field("groupName").match("表格")
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testMatchPhraseSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("groupName").matchPhrase("存储")
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testRangeSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("point").lessEqual(4D)
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(5, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testBooleanSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("isPublic").equals(true)
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(5, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testTermSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("tags").equals("Table")
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testTermsSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("tags").in("Table", "Store")
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testWildcardSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("tags").matchWildcard("Tab*")
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testPrefixSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                field("tags").startsWith("Sto")
        ).calculateTotalCount().limit(2);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testAndSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                and(
                        field("groupName").match("表格"),
                        field("createTime").greaterEqual(0),
                        field("isPublic").equals(true),
                        field("point").lessEqual(10D),
                        field("tags").in("Table", "Store")
                )
        ).calculateTotalCount().limit(6);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(5, result.getTotalCount());
        assertEquals(5, result.getEntries().size());
        assertNull(result.getNextToken());
    }

    @Test
    public void testOrSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                or(
                        field("groupName").match("表格"),
                        field("createTime").greaterEqual(0),
                        field("isPublic").equals(true),
                        field("point").lessEqual(10D),
                        field("tags").in("Table", "Store")
                )
        ).calculateTotalCount().limit(5);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(10, result.getTotalCount());
        assertEquals(5, result.getEntries().size());
    }

    @Test
    public void testNotSearchMeta() {
        SearchParameter parameter = new SearchParameter(
                not(
                        field("createTime").lessEqual(6),
                        field("isPublic").equals(true),
                        field("point").lessEqual(6D)
                )
        ).calculateTotalCount().limit(5);

        SearchResult<TimelineMeta> result = metaService.search(parameter);
        assertEquals(2, result.getTotalCount());
        assertEquals(2, result.getEntries().size());
    }

    @Test
    public void testAndSearchTimeline() {
        SearchParameter parameter = new SearchParameter(
                and(
                        field("timelineId").equals("group_1"),
                        field("text").match("hello"),
                        field("timestamp").lessEqual(2),
                        field("from").equals("ots"),
                        field("receivers").in("user_a", "user_b")
                )
        ).calculateTotalCount().limit(5);

        SearchResult<TimelineEntry> result = timelineStore.search(parameter);
        assertEquals(3, result.getTotalCount());
        assertEquals(3, result.getEntries().size());
    }

    @Test
    public void testOrSearchTimeline() {
        SearchParameter parameter = new SearchParameter(
                or(
                        field("timelineId").equals("group_1"),
                        field("text").match("hello"),
                        field("timestamp").lessEqual(2),
                        field("from").equals("ots"),
                        field("receivers").in("user_a", "user_b")
                )
        ).calculateTotalCount().limit(5);

        SearchResult<TimelineEntry> result = timelineStore.search(parameter);
        assertEquals(20, result.getTotalCount());
        assertEquals(5, result.getEntries().size());
        assertNotNull(result.getNextToken());
    }

    @Test
    public void testNotSearchTimeline() {
        SearchParameter parameter = new SearchParameter(
                not(
                        field("timelineId").equals("group_3"),
                        field("text").match("hello"),
                        field("timestamp").lessEqual(2),
                        field("from").equals("ots123")
                )
        ).calculateTotalCount().limit(5);

        SearchResult<TimelineEntry> result = timelineStore.search(parameter);
        assertEquals(7, result.getTotalCount());
        assertEquals(5, result.getEntries().size());
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
