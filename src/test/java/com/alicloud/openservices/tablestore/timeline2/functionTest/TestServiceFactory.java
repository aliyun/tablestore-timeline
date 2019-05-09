package com.alicloud.openservices.tablestore.timeline2.functionTest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.common.ServiceWrapper;
import org.junit.*;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestServiceFactory {
    private static TimelineMetaStore metaService = null;
    private static TimelineStore storeTableService = null;
    private static TimelineStore syncTableService = null;
    private static SyncClient syncClient = null;

    @BeforeClass
    public static void setUp() throws Exception {
        ServiceWrapper wrapper = ServiceWrapper.newInstance();
        syncClient = wrapper.getSyncClient();
        metaService = wrapper.getMetaStore();
        storeTableService = wrapper.getStoreTableStore();
        syncTableService = wrapper.getSyncTableStore();
    }

    @AfterClass
    public static void after() throws Exception {
        storeTableService.flush();
        syncTableService.flush();
        syncClient.shutdown();
    }

    @Test
    public void testCreateTablesAndIndex() {
        /**
         * Create tables and index
         * */
        metaService.prepareTables();
        storeTableService.prepareTables();
        syncTableService.prepareTables();

        List<String> tableNames = syncClient.listTable().getTableNames();
        assertTrue("metaTable should exist", tableNames.contains("metaTable"));
        assertTrue("storeTable should exist", tableNames.contains("storeTable"));
        assertTrue("syncTable should exist", tableNames.contains("syncTable"));


        ListSearchIndexRequest listMetaSearchIndexRequest = new ListSearchIndexRequest();
        listMetaSearchIndexRequest.setTableName("metaTable");
        ListSearchIndexResponse listMetaSearchIndexResponse = syncClient.listSearchIndex(listMetaSearchIndexRequest);

        assertEquals("metaTableIndex should exist", "metaTableIndex", listMetaSearchIndexResponse.getIndexInfos().get(0).getIndexName());

        ListSearchIndexRequest listStoreSearchIndexRequest = new ListSearchIndexRequest();
        listStoreSearchIndexRequest.setTableName("storeTable");
        ListSearchIndexResponse listStoreSearchIndexResponse = syncClient.listSearchIndex(listStoreSearchIndexRequest);

        assertEquals("storeTableIndex should exist", "storeTableIndex", listStoreSearchIndexResponse.getIndexInfos().get(0).getIndexName());

        /**
         * Delete tables and indexs
         * */
        metaService.dropAllTables();
        storeTableService.dropAllTables();
        syncTableService.dropAllTables();

        List<String> afterDropTableNames = syncClient.listTable().getTableNames();
        assertFalse("metaTable should dropped", afterDropTableNames.contains("metaTable"));
        assertFalse("storeTable should dropped", afterDropTableNames.contains("storeTable"));
        assertFalse("syncTable should dropped", afterDropTableNames.contains("syncTable"));
    }
}
