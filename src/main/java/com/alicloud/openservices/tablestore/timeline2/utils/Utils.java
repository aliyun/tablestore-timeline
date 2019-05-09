package com.alicloud.openservices.tablestore.timeline2.utils;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.search.SearchQuery;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import com.alicloud.openservices.tablestore.timeline2.model.*;
import com.alicloud.openservices.tablestore.timeline2.model.RowPutChangeWithCallback;
import com.alicloud.openservices.tablestore.timeline2.query.SearchParameter;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;

public class Utils {
    public static SearchQuery toSearchQuery(SearchParameter searchParam) {
        SearchQuery query = new SearchQuery();
        query.setGetTotalCount(searchParam.isCalculateTotalCount());
        query.setLimit(searchParam.getLimit());
        query.setOffset(searchParam.getOffset());
        query.setQuery(searchParam.getFieldCondition().getQuery());
        query.setSort(searchParam.getSort());
        if (searchParam.getToken() != null) {
            query.setToken(searchParam.getToken());
        }
        return query;
    }

    public static Row metaToRow(TimelineMeta meta) {
        PrimaryKey primaryKey = identifierToPrimaryKey(meta.getIdentifier());
        Collection<Column> fields = meta.getFields().values();
        return new Row(primaryKey, fields.toArray(new Column[fields.size()]));
    }

    public static RowPutChange messageToRowPutChange(String tableName, PrimaryKey primaryKey, TimelineMessage message) {
        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);
        for (Column column : message.getFields().values()) {
            rowChange.addColumn(column);
        }

        return rowChange;
    }

    public static RowPutChangeWithCallback messageToNewRowPutChange(String tableName, PrimaryKey primaryKey, TimelineMessage message) {
        RowPutChangeWithCallback rowChange = new RowPutChangeWithCallback(tableName, primaryKey);
        for (Column column : message.getFields().values()) {
            rowChange.addColumn(column);
        }
        rowChange.setReturnType(ReturnType.RT_PK);

        return rowChange;
    }

    public static PrimaryKey identifierToPrimaryKeyWithSequenceId(TimelineIdentifier identifier, String seqColName,
                                                                  long sequenceId, boolean isAutoIncrement) {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (PrimaryKeyColumn column : identifier.getFields()) {
            builder.addPrimaryKeyColumn(column);
        }

        builder.addPrimaryKeyColumn(
                isAutoIncrement ? new PrimaryKeyColumn(seqColName, PrimaryKeyValue.AUTO_INCREMENT)
                        : new PrimaryKeyColumn(seqColName, PrimaryKeyValue.fromLong(sequenceId))
        );

        return builder.build();
    }

    public static PrimaryKey identifierToPrimaryKey(TimelineIdentifier identifier) {
        PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (PrimaryKeyColumn column : identifier.getFields()) {
            builder.addPrimaryKeyColumn(column);
        }

        return builder.build();
    }

    public static TimelineIdentifier primaryKeyToIdentifier(TimelineIdentifierSchema identifierSchema, PrimaryKey primaryKey) {
        TimelineIdentifier.Builder builder = new TimelineIdentifier.Builder();
        for (int i = 0; i < identifierSchema.getKeys().size(); i++) {
            PrimaryKeySchema keySchema = identifierSchema.getKeys().get(i);
            PrimaryKeyColumn column = primaryKey.getPrimaryKeyColumn(i);
            if (!keySchema.getName().equals(column.getName())) {
                throw new TimelineException("Identifier schema not match primary key schema.");
            }

            builder.addField(column);
        }

        return builder.build();
    }

    public static TimelineMeta rowToMeta(TimelineIdentifierSchema identifierSchema, Row row) {
        if (row == null) {
            return null;
        }

        TimelineIdentifier identifier = primaryKeyToIdentifier(identifierSchema, row.getPrimaryKey());
        TimelineMeta timelineMeta = new TimelineMeta(identifier);
        for (Column column : row.getColumns()) {
            timelineMeta.setField(column);
        }

        return timelineMeta;
    }

    public static TimelineEntry rowToTimelineEntry(TimelineSchema schema, Row row) {
        if (row == null) {
            return null;
        }

        PrimaryKeyColumn seqCol = row.getPrimaryKey().getPrimaryKeyColumn(schema.getSequenceIdColumnName());
        Preconditions.checkArgument(seqCol != null, "Invalid schema, can not find sequence column.");
        long sequenceId = seqCol.getValue().asLong();

        TimelineMessage message = new TimelineMessage();
        for (Column column : row.getColumns()) {
            message.setField(column);
        }

        return new TimelineEntry(sequenceId, message);
    }

    /**
     * Can't get columns when update row, should use message to construct timelineEntry
     * */
    public static TimelineEntry rowToTimelineEntryWithMessage(TimelineSchema schema, Row row, TimelineMessage message) {
        if (row == null) {
            return null;
        }

        PrimaryKeyColumn seqCol = row.getPrimaryKey().getPrimaryKeyColumn(schema.getSequenceIdColumnName());
        Preconditions.checkArgument(seqCol != null, "Invalid schema, can not find sequence column.");
        long sequenceId = seqCol.getValue().asLong();

        return new TimelineEntry(sequenceId, message);
    }

    /**
     * Can't get columns by writer, should use column list to construct timelineEntry
     * */
    public static TimelineEntry rowToTimelineEntryWithColumnList(TimelineSchema schema, Row row, List<Column> columnList) {
        if (row == null) {
            return null;
        }
        TimelineMessage message = new TimelineMessage()
                .setFields(columnList);

        return rowToTimelineEntryWithMessage(schema, row, message);
    }

    public static TimelineException convertException(Exception e) {
        if (e instanceof TableStoreException) {
            TableStoreException ex = (TableStoreException)e;
            return new TimelineException(ex.getErrorCode(), ex);
        } else if (e instanceof ClientException) {
            return new TimelineException("ClientError", e);
        } else {
            return new TimelineException("OtherError", e);
        }
    }

    public static String getLocalIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new TimelineException("Can not get local machine ip.");
        }
    }

    public static String getProcessID() {
        String value = ManagementFactory.getRuntimeMXBean().getName();
        return value.substring(0, value.indexOf("@"));
    }
}
