package com.alicloud.openservices.tablestore.timeline2.core;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timeline2.TimelineQueue;
import com.alicloud.openservices.tablestore.timeline2.TimelineCallback;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.model.RowPutChangeWithCallback;
import com.alicloud.openservices.tablestore.timeline2.query.ScanParameter;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;
import com.alicloud.openservices.tablestore.timeline2.utils.Utils;

import java.util.Iterator;
import java.util.concurrent.*;

public class TimelineQueueImpl implements TimelineQueue {
    private SyncClientInterface client;
    private AsyncClientInterface asyncClient;
    private TimelineSchema schema;
    private TimelineIdentifier identifier;
    private TableStoreWriter writer;

    public TimelineQueueImpl(SyncClientInterface client, TableStoreWriter writer, TimelineSchema schema, TimelineIdentifier identifier) {
        this.client = client;
        this.writer = writer;
        this.asyncClient = client.asAsyncClient();
        this.schema = schema;
        this.identifier = identifier;
    }

    @Override
    public TimelineIdentifier getIdentifier() {
        return identifier;
    }

    @Override
    public TimelineEntry store(TimelineMessage message) {
        Preconditions.checkArgument(schema.isAutoGenerateSeqId(),
                "The sequence id of this timeline is not auto generated.");

        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, this.schema.getSequenceIdColumnName(),
                -1, schema.isAutoGenerateSeqId());
        RowPutChange rowChange = new RowPutChange(schema.getTableName(), primaryKey);
        PutRowRequest request = new PutRowRequest();
        for (String columnName : message.getFields().keySet()) {
            Column column = message.getFields().get(columnName);
            rowChange.addColumn(column);
        }

        rowChange.setReturnType(ReturnType.RT_PK);
        request.setRowChange(rowChange);

        long sequenceId;
        try {
            PutRowResponse response = client.putRow(request);
            sequenceId = response.getRow()
                    .getPrimaryKey()
                    .getPrimaryKeyColumn(schema.getSequenceIdColumnName())
                    .getValue()
                    .asLong();
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        return new TimelineEntry(sequenceId, message);
    }

    @Override
    public TimelineEntry store(long sequenceId, TimelineMessage message) {
        Preconditions.checkArgument(!schema.isAutoGenerateSeqId(),
                "The sequence id of this timeline is not auto generated.");

        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, this.schema.getSequenceIdColumnName(),
                sequenceId, schema.isAutoGenerateSeqId());
        RowPutChange rowChange = new RowPutChange(schema.getTableName(), primaryKey);
        PutRowRequest request = new PutRowRequest();
        for (String columnName : message.getFields().keySet()) {
            Column column = message.getFields().get(columnName);
            rowChange.addColumn(column);
        }

        request.setRowChange(rowChange);

        try {
            client.putRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        return new TimelineEntry(sequenceId, message);
    }

    @Override
    public Future<TimelineEntry> storeAsync(TimelineMessage message, TimelineCallback callback) {
        Preconditions.checkArgument(schema.isAutoGenerateSeqId(),
                "The sequence id of this timeline is not auto generated.");

        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, this.schema.getSequenceIdColumnName(),
                -1, schema.isAutoGenerateSeqId());
        RowPutChange rowChange = new RowPutChange(schema.getTableName(), primaryKey);
        PutRowRequest request = new PutRowRequest();
        for (String columnName : message.getFields().keySet()) {
            Column column = message.getFields().get(columnName);
            rowChange.addColumn(column);
        }

        rowChange.setReturnType(ReturnType.RT_PK);
        request.setRowChange(rowChange);

        return doStoreAsync(-1, message, request, callback);
    }


    @Override
    public Future<TimelineEntry> storeAsync(long sequenceId, TimelineMessage message, TimelineCallback callback) {
        Preconditions.checkArgument(!schema.isAutoGenerateSeqId(),
                "The sequence id of this timeline is not allowed to set manually.");

        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, this.schema.getSequenceIdColumnName(),
                sequenceId, schema.isAutoGenerateSeqId());
        RowPutChange rowChange = new RowPutChange(schema.getTableName(), primaryKey);
        PutRowRequest request = new PutRowRequest();
        for (String columnName : message.getFields().keySet()) {
            Column column = message.getFields().get(columnName);
            rowChange.addColumn(column);
        }

        rowChange.setReturnType(ReturnType.RT_PK);
        request.setRowChange(rowChange);
        return doStoreAsync(sequenceId, message, request, callback);
    }

    @Override
    public Future<TimelineEntry> batchStore(TimelineMessage message) {
        return batchStore(message, null);
    }

    @Override
    public Future<TimelineEntry> batchStore(long sequenceId, TimelineMessage message) {
        return batchStore(sequenceId, message, null);
    }


    @Override
    public Future<TimelineEntry> batchStore(TimelineMessage message, TimelineCallback callback) {
        Preconditions.checkArgument(schema.isAutoGenerateSeqId(),
                "The sequence id of this timeline is not auto generated.");
        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, schema.getSequenceIdColumnName(),
                -1, schema.isAutoGenerateSeqId());

        return doBatchWriteAsync(primaryKey, message, callback);
    }

    @Override
    public Future<TimelineEntry> batchStore(long sequenceId, TimelineMessage message, TimelineCallback callback) {
        Preconditions.checkArgument(!schema.isAutoGenerateSeqId(),
                "The sequence id of this timeline is not allowed to set manually.");
        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, schema.getSequenceIdColumnName(),
                sequenceId, schema.isAutoGenerateSeqId());

        return doBatchWriteAsync(primaryKey, message, callback);
    }

    @Override
    public TimelineEntry update(long sequenceId, TimelineMessage message) {
        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, this.schema.getSequenceIdColumnName(),
                sequenceId, false);
        RowUpdateChange rowChange = new RowUpdateChange(schema.getTableName(), primaryKey);
        for (String columnName : message.getFields().keySet()) {
            Column column = message.getFields().get(columnName);
            rowChange.put(column);
        }

        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(rowChange);

        try {
            client.updateRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        return new TimelineEntry(sequenceId, message);
    }

    @Override
    public Future<TimelineEntry> updateAsync(long sequenceId, TimelineMessage message, TimelineCallback callback) {
        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, this.schema.getSequenceIdColumnName(),
                sequenceId, false);
        RowUpdateChange rowChange = new RowUpdateChange(schema.getTableName(), primaryKey);
        for (String columnName : message.getFields().keySet()) {
            Column column = message.getFields().get(columnName);
            rowChange.put(column);
        }
        rowChange.setReturnType(ReturnType.RT_PK);

        UpdateRowRequest request = new UpdateRowRequest();
        request.setRowChange(rowChange);

        return doUpdateAsync(sequenceId, message, request, callback);
    }

    @Override
    public TimelineEntry get(long sequenceId) {
        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, schema.getSequenceIdColumnName(),
                sequenceId, false);

        GetRowRequest request = new GetRowRequest();
        SingleRowQueryCriteria singleRowQueryCriteria =new SingleRowQueryCriteria(schema.getTableName(), primaryKey);
        singleRowQueryCriteria.setMaxVersions(1);

        request.setRowQueryCriteria(singleRowQueryCriteria);

        GetRowResponse response;
        try {
            response = client.getRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        return Utils.rowToTimelineEntry(schema, response.getRow());
    }

    @Override
    public void delete(long sequenceId) {
        PrimaryKey primaryKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, schema.getSequenceIdColumnName(),
                sequenceId, false);
        RowDeleteChange rowChange = new RowDeleteChange(schema.getTableName(), primaryKey);

        DeleteRowRequest request = new DeleteRowRequest();
        request.setRowChange(rowChange);

        try {
            client.deleteRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
    }

    @Override
    public Iterator<TimelineEntry> scan(ScanParameter parameter) {
        RangeIteratorParameter param = new RangeIteratorParameter(schema.getTableName());
        param.setMaxVersions(1);
        param.setMaxCount(parameter.getMaxCount());
        param.setDirection(parameter.isForward() ? Direction.FORWARD : Direction.BACKWARD);
        if (parameter.getFilter() != null) {
            param.setFilter(parameter.getFilter());
        }

        PrimaryKey startKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, schema.getSequenceIdColumnName(), parameter.getFrom(), false);
        PrimaryKey endKey = Utils.identifierToPrimaryKeyWithSequenceId(identifier, schema.getSequenceIdColumnName(), parameter.getTo(), false);
        param.setInclusiveStartPrimaryKey(startKey);
        param.setExclusiveEndPrimaryKey(endKey);

        TimelineEntryIterator timelineEntryIterator;

        try {
            timelineEntryIterator = new TimelineEntryIterator(client, param, schema);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        return timelineEntryIterator;
    }

    @Override
    public long getLatestSequenceId() {
        TimelineEntry timelineEntry = getLatestTimelineEntry();
        if (timelineEntry != null) {
            return timelineEntry.getSequenceID();
        }

        return 0;
    }

    @Override
    public TimelineEntry getLatestTimelineEntry() {
        Iterator<TimelineEntry> iterator = scan(new ScanParameter()
            .scanBackward(Long.MAX_VALUE, 0)
            .maxCount(1));

        if (iterator.hasNext()) {
            return iterator.next();
        }

        return null;
    }


    private Future<TimelineEntry> doStoreAsync(final long sequenceId, final TimelineMessage message, final PutRowRequest request, final TimelineCallback callback)
    {
        TableStoreCallback<PutRowRequest, PutRowResponse> tableStoreCallback = null;
        if (callback != null) {
            tableStoreCallback = new TableStoreCallback<PutRowRequest, PutRowResponse>() {
                @Override
                public void onCompleted(PutRowRequest request, PutRowResponse response) {
                    long finalSequenceId = sequenceId;
                    if (schema.isAutoGenerateSeqId()) {
                        finalSequenceId = response.getRow()
                                .getPrimaryKey()
                                .getPrimaryKeyColumn(schema.getSequenceIdColumnName())
                                .getValue()
                                .asLong();
                    }
                    TimelineEntry timelineEntry = new TimelineEntry(finalSequenceId, message);
                    callback.onCompleted(identifier, message, timelineEntry);
                }

                @Override
                public void onFailed(PutRowRequest request, Exception e) {
                    e = Utils.convertException(e);
                    callback.onFailed(identifier, message, e);
                }
            };
        }

        final Future<PutRowResponse> future = asyncClient.putRow(request, tableStoreCallback);
        return new Future<TimelineEntry>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return future.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }

            @Override
            public boolean isDone() {
                return future.isDone();
            }

            @Override
            public TimelineEntry get() throws InterruptedException, ExecutionException {
                PutRowResponse response;
                try {
                    response = future.get();
                } catch (InterruptedException e) {
                    throw e;
                } catch (ExecutionException e) {
                    throw e;
                }  catch (Exception e) {
                    throw Utils.convertException(e);
                }
                return Utils.rowToTimelineEntryWithMessage(schema, response.getRow(), message);
            }

            @Override
            public TimelineEntry get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                PutRowResponse response;
                try {
                    response = future.get(timeout, unit);
                } catch (TimeoutException e) {
                    throw e;
                } catch (InterruptedException e) {
                    throw e;
                } catch (ExecutionException e) {
                    throw e;
                }  catch (Exception e) {
                    throw Utils.convertException(e);
                }
                return Utils.rowToTimelineEntryWithMessage(schema, response.getRow(), message);
            }
        };
    }

    private Future<TimelineEntry> doUpdateAsync(final Long sequenceId, final TimelineMessage message, final UpdateRowRequest request, final TimelineCallback callback)
    {
        TableStoreCallback<UpdateRowRequest, UpdateRowResponse> tableStoreCallback = null;
        if (callback != null) {
            tableStoreCallback = new TableStoreCallback<UpdateRowRequest, UpdateRowResponse> () {
                @Override
                public void onCompleted(UpdateRowRequest request, UpdateRowResponse response) {
                    TimelineEntry timelineEntry = new TimelineEntry(sequenceId, message);
                    callback.onCompleted(identifier, message, timelineEntry);
                }

                @Override
                public void onFailed(UpdateRowRequest request, Exception e) {
                    e = Utils.convertException(e);
                    callback.onFailed(identifier, message, e);
                }
            };
        }

        final Future<UpdateRowResponse> future = asyncClient.updateRow(request, tableStoreCallback);
        return new Future<TimelineEntry>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return future.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return future.isCancelled();
            }

            @Override
            public boolean isDone() {
                return future.isDone();
            }

            @Override
            public TimelineEntry get() throws InterruptedException, ExecutionException {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    throw e;
                } catch (ExecutionException e) {
                    throw e;
                }  catch (Exception e) {
                    throw Utils.convertException(e);
                }
                return new TimelineEntry(sequenceId, message);
            }

            @Override
            public TimelineEntry get(long timeout, TimeUnit unit) throws TimelineException, InterruptedException, ExecutionException, TimeoutException {
                try {
                    future.get(timeout, unit);
                } catch (TimeoutException e) {
                    throw e;
                } catch (InterruptedException e) {
                    throw e;
                } catch (ExecutionException e) {
                    throw e;
                }  catch (Exception e) {
                    throw Utils.convertException(e);
                }
                return new TimelineEntry(sequenceId, message);
            }
        };
    }

    private Future<TimelineEntry> doBatchWriteAsync(PrimaryKey primaryKey, TimelineMessage message, TimelineCallback callback) {
        RowPutChangeWithCallback rowChange = Utils.messageToNewRowPutChange(schema.getTableName(), primaryKey,  message)
                .withTimelineIdentifier(identifier);

        if (callback != null) {
            rowChange.watchBy(callback);
        }

        writer.addRowChange(rowChange);

        return rowChange.getFuture();
    }

    @Override
    public void flush() {
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public void close() {
        // do nothing, both of SyncClient and AsyncClient should be shutdown outside;
    }
}
