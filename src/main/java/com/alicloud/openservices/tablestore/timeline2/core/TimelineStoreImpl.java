package com.alicloud.openservices.tablestore.timeline2.core;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineStore;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.model.RowPutChangeWithCallback;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;
import com.alicloud.openservices.tablestore.timeline2.utils.Utils;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class TimelineStoreImpl implements TimelineStore {

    private final SyncClientInterface client;
    private final AsyncClientInterface asyncClient;
    private final TimelineSchema schema;

    private ExecutorService threadPool;
    private TableStoreWriter writer;
    private TableStoreCallback<RowChange, ConsumedCapacity> callback = null;
    private TableStoreCallback<RowChange, RowWriteResult> resultCallback = new TableStoreCallback<RowChange, RowWriteResult>() {
        @Override
        public void onCompleted(RowChange req, RowWriteResult res) {
            if (req instanceof RowPutChangeWithCallback) {
                RowPutChangeWithCallback rowPutChange = (RowPutChangeWithCallback) req;
                TimelineEntry timelineEntry = Utils.rowToTimelineEntryWithColumnList(schema, res.getRow(), rowPutChange.getColumnsToPut());

                rowPutChange.setComplete(timelineEntry);
            }
        }

        @Override
        public void onFailed(RowChange req, Exception ex) {
            if (req instanceof RowPutChangeWithCallback) {
                RowPutChangeWithCallback rowPutChange = (RowPutChangeWithCallback) req;

                rowPutChange.setFailed(ex);
            }
        }
    };


    public TimelineStoreImpl(SyncClient client, final TimelineSchema schema) {
        this.client = client;
        this.asyncClient = client.asAsyncClient();
        this.schema = schema;
    }

    @Override
    public TimelineQueue createTimelineQueue(TimelineIdentifier identifier) {
        Preconditions.checkNotNull(identifier, "Identifier should not be null.");

        if (null == writer) {
            synchronized (this) {
                if (null == writer) {
                    ThreadFactory threadFactory = new ThreadFactory() {
                        private final AtomicInteger counter = new AtomicInteger(1);
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "timeline-callback-" + counter.getAndIncrement());
                        }
                    };

                    //common thread pool
                    threadPool = new ThreadPoolExecutor(schema.getCallbackExecuteThreads(), schema.getMaxCallbackExecuteThreads(),
                            0L, TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue(1024), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

                    writer = new DefaultTableStoreWriter(asyncClient, schema.getTableName(), schema.getWriterConfig(), callback, threadPool);
                    writer.setResultCallback(resultCallback);
                }
            }
        }

        return new TimelineQueueImpl(client, writer, schema, identifier);
    }

    @Override
    public SearchResult<TimelineEntry> search(SearchParameter searchParameter) {
        return search(Utils.toSearchQuery(searchParameter));
    }

    @Override
    public SearchResult<TimelineEntry> search(SearchQuery searchQuery) {
        Preconditions.checkArgument(schema.hasDataIndex(), "The store not support search cause not has data index");

        SearchRequest request = new SearchRequest(schema.getTableName(), schema.getIndexName(), searchQuery);
        SearchRequest.ColumnsToGet columnsToGet = new SearchRequest.ColumnsToGet();
        columnsToGet.setReturnAll(true);
        request.setColumnsToGet(columnsToGet);

        SearchResponse response;
        try {
            response = client.search(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        List<SearchResult.Entry<TimelineEntry>> entries = new ArrayList<SearchResult.Entry<TimelineEntry>>(response.getRows().size());
        for (Row row : response.getRows()) {
            TimelineEntry entry = Utils.rowToTimelineEntry(schema, row);
            TimelineIdentifier identifier = Utils.primaryKeyToIdentifier(schema.getIdentifierSchema(), row.getPrimaryKey());
            SearchResult.Entry<TimelineEntry> se = new SearchResult.Entry<TimelineEntry>(identifier, entry);
            entries.add(se);
        }
        SearchResult<TimelineEntry> result = new SearchResult<TimelineEntry>(
                entries, response.isAllSuccess(),
                response.getTotalCount(), response.getNextToken());
        return  result;
    }

    @Override
    public void prepareTables() {
        // create table
        TableMeta tableMeta = new TableMeta(schema.getTableName());
        for (PrimaryKeySchema key : schema.getIdentifierSchema().getKeys()) {
            tableMeta.addPrimaryKeyColumn(key);
        }

        if (schema.isAutoGenerateSeqId()) {
            tableMeta.addAutoIncrementPrimaryKeyColumn(schema.getSequenceIdColumnName());
        } else {
            tableMeta.addPrimaryKeyColumn(schema.getSequenceIdColumnName(), PrimaryKeyType.INTEGER);
        }

        TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(schema.getTimeToLive());
        tableOptions.setMaxVersions(1);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        try {
            client.createTable(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        // create searchIndex if necessary
        if (schema.hasDataIndex()) {
            CreateSearchIndexRequest csRequest = new CreateSearchIndexRequest();
            csRequest.setTableName(schema.getTableName());
            csRequest.setIndexName(schema.getIndexName());
            csRequest.setIndexSchema(schema.getIndexSchema());

            try {
                client.createSearchIndex(csRequest);
            } catch (Exception e) {
                throw Utils.convertException(e);
            }
        }
    }

    @Override
    public void dropAllTables() {
        // delete searchIndex if necessary
        if (schema.hasDataIndex()) {
            DeleteSearchIndexRequest dsRequest = new DeleteSearchIndexRequest();
            dsRequest.setTableName(schema.getTableName());
            dsRequest.setIndexName(schema.getIndexName());

            try {
                client.deleteSearchIndex(dsRequest);
            } catch (Exception e) {
                throw Utils.convertException(e);
            }
        }

        // delete timeline table
        DeleteTableRequest request = new DeleteTableRequest(schema.getTableName());
        try {
            client.deleteTable(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
    }

    @Override
    public void flush() {
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public void close() {
        if (writer != null) {
            writer.close();
            threadPool.shutdown();
        }

    }
}
