package examples.v2;

import com.alicloud.openservices.tablestore.ClientConfiguration;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.model.search.query.TermQuery;
import com.alicloud.openservices.tablestore.timeline2.*;
import com.alicloud.openservices.tablestore.timeline2.core.TimelineStoreFactoryImpl;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.query.ScanParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;

import java.util.Arrays;

import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.field;

public class TimelineSample {
    private TimelineStoreFactory serviceFactory;
    private TimelineMetaStore timelineMetaStore;
    private TimelineStore timelineStore;
    private TimelineQueue timelineQueue;

    public static void main(String[] args) {
        TimelineSample sample = new TimelineSample();

        sample.getFactory();

        sample.getMetaStore();
        sample.getTimelineStore();

        sample.howToUseMetaStoreApi();
        sample.howToUseTimelineStoreApi();
        sample.howToUseTimelineQueueApi();
    }

    public void getFactory() {
        /**
         * Support user-defined retry strategy by implement RetryStrategy.
         *
         * Code: configuration.setRetryStrategy(new DefaultRetryStrategy());
         * */
        ClientConfiguration configuration = new ClientConfiguration();

        SyncClient client = new SyncClient(
                "http://instanceName.cn-shanghai.ots.aliyuncs.com",
                "accessKeyId",
                "accessKeySecret",
                "instanceName", configuration);

        serviceFactory = new TimelineStoreFactoryImpl(client);
    }

    public void getMetaStore() {
        TimelineIdentifierSchema idSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("timeline_id").build();

        // index schema of meta table, take group meta for example
        IndexSchema metaIndex = new IndexSchema();
        metaIndex.addFieldSchema(
                new FieldSchema("group_name", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord)
        );

        // set timeline schema and prepare all tables include data table/index and meta table/index.
        TimelineMetaSchema metaSchema = new TimelineMetaSchema("groupMeta", idSchema)
                .withIndex("metaIndex", metaIndex);

        timelineMetaStore = serviceFactory.createMetaStore(metaSchema);
    }

    public void getTimelineStore() {
        TimelineIdentifierSchema idSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("timeline_id").build();

        // index schema of timeline table
        IndexSchema timelineIndex = new IndexSchema();
        timelineIndex.setFieldSchemas(Arrays.asList(
                new FieldSchema("text", FieldType.TEXT).setIndex(true).setAnalyzer(FieldSchema.Analyzer.MaxWord),
                new FieldSchema("receivers", FieldType.KEYWORD).setIndex(true).setIsArray(true)
        ));

        // set timeline schema and prepare all tables include data table/index and meta table/index.
        TimelineSchema timelineSchema = new TimelineSchema("timeline", idSchema)
                .autoGenerateSeqId() //set auto-generated sequence id
                .withIndex("metaIndex", timelineIndex);

        timelineStore = serviceFactory.createTimelineStore(timelineSchema);
    }

    public void howToUseMetaStoreApi() {
        /**
         * Prepare important parameter.
         * */
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timeline_id", "group")
                .build();
        TimelineMeta meta = new TimelineMeta(identifier)
                .setField("filedName", "fieldValue");

        /**
         * Create table and searchIndex before CRUD.
         * */
        timelineMetaStore.prepareTables();

        /**
         * Insert meta.
         * */
        timelineMetaStore.insert(meta);

        /**
         * Read meta.
         * */
        timelineMetaStore.read(identifier);

        /**
         * Update meta.
         * */
        meta.setField("fieldName", "newValue");
        timelineMetaStore.update(meta);

        /**
         * Delete meta.
         * */
        timelineMetaStore.delete(identifier);

        /**
         * Search meta by SearchParameter.
         * */
        SearchParameter parameter = new SearchParameter(
                field("fieldName").equals("fieldValue")
        );
        timelineMetaStore.search(parameter);

        /**
         * Search meta by SearchQuery.
         * */
        TermQuery query = new TermQuery();
        query.setFieldName("fieldName");
        query.setTerm(ColumnValue.fromString("fieldValue"));

        SearchQuery searchQuery = new SearchQuery().setQuery(query);
        timelineMetaStore.search(searchQuery);

        /**
         * Delete meta table and searchIndex.
         * */
        timelineMetaStore.dropAllTables();
    }

    public void howToUseTimelineStoreApi() {
        /**
         * Prepare important parameters.
         * */
        SearchParameter searchParameter = new SearchParameter(
                field("text").equals("fieldValue")
        );

        TermQuery query = new TermQuery();
        query.setFieldName("text");
        query.setTerm(ColumnValue.fromString("fieldValue"));
        SearchQuery searchQuery = new SearchQuery().setQuery(query).setLimit(10);

        /**
         * Create table and searchIndex before CRUD.
         * */
        timelineStore.prepareTables();


        /**
         * Search timeline by SearchParameter.
         * */
        timelineStore.search(searchParameter);

        /**
         * Search timeline by SearchQuery.
         * */

        timelineStore.search(searchQuery);

        /**
         * Flush messages in buffer, and wait until all messages are stored.
         * */
        timelineStore.flush();

        /**
         * Close writer with thread pool.
         * */
        timelineStore.close();

        /**
         * Delete timeline table and searchIndex.
         * */
        timelineStore.dropAllTables();
    }

    private void howToUseTimelineQueueApi() {
        /**
         * Prepare important parameters.
         * */
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("timeline_id", "group")
                .build();
        long sequenceId = 1557133858994L;
        TimelineMessage message = new TimelineMessage().setField("text", "Timeline is fine.");
        ScanParameter scanParameter = new ScanParameter().scanBackward(Long.MAX_VALUE, 0);
        TimelineCallback callback = new TimelineCallback() {
            @Override
            public void onCompleted(TimelineIdentifier i, TimelineMessage m, TimelineEntry t) {
                // do something when succeed.
            }

            @Override
            public void onFailed(TimelineIdentifier i, TimelineMessage m, Exception e) {
                // do something when failed.
            }
        };

        /**
         * Get single-timeline-queue service with specified identifier.
         * */
        timelineQueue = timelineStore.createTimelineQueue(identifier);

        /**
         * Store timeline.
         * */
        //synchronous store, and return TimelineEntry.
        timelineQueue.store(message);
        timelineQueue.store(sequenceId, message);
        //asynchronous store with callback, and return Future.
        timelineQueue.storeAsync(message, callback);
        timelineQueue.storeAsync(sequenceId, message, callback);
        //asynchronous batch store without callback, and return Future.
        timelineQueue.batchStore(message);
        timelineQueue.batchStore(sequenceId, message);
        //asynchronous batch store with callback, and return Future.
        timelineQueue.batchStore(message, callback);
        timelineQueue.batchStore(sequenceId, message, callback);

        /**
         * Read timeline, latest timeline and latest sequence id.
         * */
        timelineQueue.get(sequenceId);
        timelineQueue.getLatestTimelineEntry();
        timelineQueue.getLatestSequenceId();

        /**
         * Update timeline.
         * */
        message.setField("text", "newValue");
        timelineQueue.update(sequenceId, message);
        timelineQueue.updateAsync(sequenceId, message, callback);

        /**
         * Delete timeline.
         * */
        timelineQueue.delete(sequenceId);

        /**
         * Scan timeline by range parameter.
         * */
        timelineQueue.scan(scanParameter);
    }
}
