package com.alicloud.openservices.tablestore.timeline2.common;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.TimelineStoreFactory;
import com.alicloud.openservices.tablestore.timeline2.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifierSchema;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMetaSchema;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineSchema;
import com.alicloud.openservices.tablestore.writer.WriterConfig;

import java.io.FileNotFoundException;

public class ServiceWrapper {
    private static TimelineMetaStore timelineMetaStore = null;
    private static TimelineStore storeTable = null;
    private static TimelineStore syncTable = null;
    private static SyncClient syncClient = null;

    private ServiceWrapper() throws FileNotFoundException {
        init();
    }

    public static ServiceWrapper newInstance() throws FileNotFoundException {
        return new ServiceWrapper();
    }

    public void init() throws FileNotFoundException {
        Conf conf = Conf.newInstance(System.getProperty("user.home") + "/timelineConf.json");
        syncClient = new SyncClient(conf.getEndpoint(), conf.getAccessId(), conf.getAccessKey(), conf.getInstanceName());
        TimelineStoreFactory tableStoreServiceFactory = new TimelineStoreFactoryImpl(syncClient);

        TimelineIdentifierSchema identifierSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("timelineId")
                .addLongField("long")
                .build();

        /**
         * init meta table service
         * */
        IndexSchema metaIndex = new IndexSchema();
        metaIndex.addFieldSchema(new FieldSchema("groupName", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        metaIndex.addFieldSchema(new FieldSchema("createTime", FieldType.LONG).setIndex(true));

        TimelineMetaSchema metaSchema = new TimelineMetaSchema("metaTable", identifierSchema)
                .withIndex("metaTableIndex", metaIndex);

        timelineMetaStore = tableStoreServiceFactory.createMetaStore(metaSchema);


        /**
         * init store table service
         * */
        IndexSchema storeIndex = new IndexSchema();
        storeIndex.addFieldSchema(new FieldSchema("text", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord));
        storeIndex.addFieldSchema(new FieldSchema("receivers", FieldType.KEYWORD).setIsArray(true).setIndex(true));
        storeIndex.addFieldSchema(new FieldSchema("timestamp", FieldType.LONG).setEnableSortAndAgg(true));


        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setFlushInterval(1000);

        TimelineSchema storeTableSchema = new TimelineSchema("storeTable", identifierSchema)
                .withIndex("storeTableIndex", storeIndex)
                .autoGenerateSeqId()
                .setSequenceIdColumnName("sequenceId")
                .setTimeToLive(-1)
                .withWriterConfig(writerConfig);

        storeTable = tableStoreServiceFactory.createTimelineStore(storeTableSchema);

        /**
         * init sync table service
         * */
        TimelineSchema syncTableSchema = new TimelineSchema("syncTable", identifierSchema)
                .manualSetSeqId()
                .setSequenceIdColumnName("sequenceId")
                .setTimeToLive(604800)
                .withWriterConfig(writerConfig);

        syncTable = tableStoreServiceFactory.createTimelineStore(syncTableSchema);
    }

    public TimelineMetaStore getMetaStore() {
        return timelineMetaStore;
    }

    public TimelineStore getStoreTableStore() {
        return storeTable;
    }

    public TimelineStore getSyncTableStore() {
        return syncTable;
    }

    public SyncClient getSyncClient() {
        return syncClient;
    }

    public static void sleepForSyncData() {
        try {
            Thread.sleep(25000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void sleepForWriterOrAsync() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        storeTable.flush();
        syncTable.flush();

        storeTable.close();
        syncTable.close();

        syncClient.shutdown();
    }
}
