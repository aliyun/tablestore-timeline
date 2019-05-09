package com.alicloud.openservices.tablestore.timeline2.core;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.*;
import com.alicloud.openservices.tablestore.timeline2.TimelineMetaStore;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineIdentifier;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMeta;
import com.alicloud.openservices.tablestore.timeline2.model.TimelineMetaSchema;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;
import com.alicloud.openservices.tablestore.timeline2.query.SearchResult;
import com.alicloud.openservices.tablestore.timeline2.utils.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TimelineMetaStoreImpl implements TimelineMetaStore {

    private final SyncClient client;
    private final TimelineMetaSchema schema;

    public TimelineMetaStoreImpl(SyncClient client, TimelineMetaSchema schema) {
        this.client = client;
        this.schema = schema;
    }

    @Override
    public TimelineMeta read(TimelineIdentifier identifier) {
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(schema.getTableName());
        criteria.setPrimaryKey(Utils.identifierToPrimaryKey(identifier));
        criteria.setMaxVersions(1);

        GetRowRequest request = new GetRowRequest(criteria);
        GetRowResponse response;

        try {
            response = client.getRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
        return Utils.rowToMeta(schema.getIdentifierSchema(), response.getRow());
    }

    @Override
    public SearchResult<TimelineMeta> search(SearchParameter searchParameter) {
        return search(Utils.toSearchQuery(searchParameter));
    }

    @Override
    public SearchResult<TimelineMeta> search(SearchQuery searchQuery) {
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

        List<SearchResult.Entry<TimelineMeta>> entries = new ArrayList<SearchResult.Entry<TimelineMeta>>(response.getRows().size());
        for (Row row : response.getRows()) {
            TimelineMeta meta = Utils.rowToMeta(schema.getIdentifierSchema(), row);
            SearchResult.Entry<TimelineMeta> entry = new SearchResult.Entry<TimelineMeta>(meta.getIdentifier(), meta);
            entries.add(entry);
        }
        SearchResult<TimelineMeta> result = new SearchResult<TimelineMeta>(
                entries, response.isAllSuccess(),
                response.getTotalCount(), response.getNextToken());
        return  result;
    }

    @Override
    public TimelineMeta insert(TimelineMeta meta) {
        Row row = Utils.metaToRow(meta);

        PutRowRequest request = new PutRowRequest();
        RowPutChange rowChange = new RowPutChange(schema.getTableName());
        rowChange.setPrimaryKey(row.getPrimaryKey());
        rowChange.addColumns(row.getColumns());
        request.setRowChange(rowChange);

        try {
            client.putRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
        return meta;
    }

    @Override
    public TimelineMeta update(TimelineMeta meta) {
        Row row = Utils.metaToRow(meta);

        UpdateRowRequest request = new UpdateRowRequest();
        RowUpdateChange rowChange = new RowUpdateChange(schema.getTableName());
        rowChange.setPrimaryKey(row.getPrimaryKey());
        rowChange.put(Arrays.asList(row.getColumns()));
        request.setRowChange(rowChange);

        try {
            client.updateRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
        return meta;
    }

    @Override
    public void delete(TimelineIdentifier identifier) {
        PrimaryKey primaryKey = Utils.identifierToPrimaryKey(identifier);

        DeleteRowRequest request = new DeleteRowRequest();
        RowDeleteChange rowChange = new RowDeleteChange(schema.getTableName());
        rowChange.setPrimaryKey(primaryKey);
        request.setRowChange(rowChange);

        try {
            client.deleteRow(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
    }

    @Override
    public void prepareTables() {
        // create meta table
        TableMeta tableMeta = new TableMeta(schema.getTableName());
        for (PrimaryKeySchema key : schema.getIdentifierSchema().getKeys()) {
            tableMeta.addPrimaryKeyColumn(key);
        }

        TableOptions tableOptions = new TableOptions();
        tableOptions.setTimeToLive(-1);
        tableOptions.setMaxVersions(1);

        CreateTableRequest request = new CreateTableRequest(tableMeta, tableOptions);
        try {
            client.createTable(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }

        // create meta searchIndex if necessary
        if (schema.hasMetaIndex()) {
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
        // delete meta searchIndex if necessary
        if (schema.hasMetaIndex()) {
            DeleteSearchIndexRequest dsRequest = new DeleteSearchIndexRequest();
            dsRequest.setTableName(schema.getTableName());
            dsRequest.setIndexName(schema.getIndexName());
            try {
                client.deleteSearchIndex(dsRequest);
            } catch (Exception e) {
                throw Utils.convertException(e);
            }
        }

        // delete meta table
        DeleteTableRequest request = new DeleteTableRequest(schema.getTableName());
        try {
            client.deleteTable(request);
        } catch (Exception e) {
            throw Utils.convertException(e);
        }
    }

    @Override
    public void close() {
        // do nothing, SyncClient should be shutdown outside;
    }
}
