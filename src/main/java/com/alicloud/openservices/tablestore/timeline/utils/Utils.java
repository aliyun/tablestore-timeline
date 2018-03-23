package com.alicloud.openservices.tablestore.timeline.utils;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.timeline.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.common.TimelineExceptionType;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.store.DistributeTimelineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

public class Utils {
    public final static int CONTENT_COLUMN_START_ID = 10000;
    public final static String SYSTEM_COLUMN_NAME_PREFIX = "__";

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static <Res> Res waitForFuture(Future<Res> f) {
        try {
            return f.get();
        } catch(InterruptedException e) {
            logger.error("The thread was interrupted", e);
            return null;
        } catch(ExecutionException e) {
            logger.error("The thread was aborted", e);
            return null;
        }
    }

    public static TimelineEntry toTimelineEntry(PutRowResponse response, IMessage message) {
        long sequenceID = response.getRow().getPrimaryKey().getPrimaryKeyColumn(1).getValue().asLong();
        return new TimelineEntry(sequenceID, message);
    }

    public static TimelineEntry toTimelineEntry(UpdateRowResponse response, IMessage message) {
        long sequenceID = response.getRow().getPrimaryKey().getPrimaryKeyColumn(1).getValue().asLong();
        return new TimelineEntry(sequenceID, message);
    }

    public static TimelineEntry toTimelineEntry(Row row, DistributeTimelineConfig config) {
        PrimaryKey pk = row.getPrimaryKey();
        int pkCount = pk.getPrimaryKeyColumns().length;
        if (pkCount != 2) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "Invalid Primary Key column count, expected:2, but:" + String.valueOf(pkCount));
        }

        IMessage message = config.getMessageInstance().newInstance();

        Long sequenceID = pk.getPrimaryKeyColumn(1).getValue().asLong();

        /**
         * Read Content Column Count.
         */
        String contentCountName = Utils.SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentCountSuffix();
        long columnCount = -1;
        long currentCount = 0;
        Column contentCountColumn = row.getLatestColumn(contentCountName);
        if (contentCountColumn != null) {
            columnCount = contentCountColumn.getValue().asLong();
        }

        Column[] columns = row.getColumns();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        String messageID = null;
        int index = CONTENT_COLUMN_START_ID;
        long crc32 = 0;
        for (Column column: columns) {
            String name = column.getName();
            if (name.startsWith(SYSTEM_COLUMN_NAME_PREFIX + config.getMessageContentSuffix()))
            {
                if (currentCount == columnCount) {
                    continue;
                }
                int columnSeqID = Integer.parseInt(name.substring(SYSTEM_COLUMN_NAME_PREFIX.length() + config.getMessageContentSuffix().length()));
                if (columnSeqID != index) {
                    throw new TimelineException(TimelineExceptionType.INVALID_USE,
                            String.format("Message Content column sequence id is wrong, expected:%d, but:%d",
                            index, columnSeqID));
                }

                byte[] value = column.getValue().asBinary();
                stream.write(value, 0, value.length);

                index++;
                currentCount++;
            } else if (name.equals(SYSTEM_COLUMN_NAME_PREFIX + config.getMessageIDColumnNameSuffix())) {
                messageID = column.getValue().asString();
            } else if (name.equals(SYSTEM_COLUMN_NAME_PREFIX + config.getColumnNameOfMessageCrc32Suffix())) {
                crc32 = column.getValue().asLong();
            } else if (!name.startsWith(SYSTEM_COLUMN_NAME_PREFIX)){
                message.addAttribute(name, column.getValue().asString());
            }
        }

        if (columnCount != -1 && currentCount != columnCount) {
            throw new TimelineException(TimelineExceptionType.ABORT,
                    String.format("Message content column is broken, expected %d, but %d", columnCount, currentCount));
        }
        byte[] content = stream.toByteArray();

        try {
            stream.close();
        } catch (IOException ex) {
            logger.error("Close ByteArrayOutputStream failed", ex);
            throw new TimelineException(TimelineExceptionType.ABORT,
                    "Close ByteArrayOutputStream failed", ex);
        }

        if (config.getColumnNameOfMessageCrc32Suffix() != null
                && !config.getColumnNameOfMessageCrc32Suffix().isEmpty())
        {
            long current = Utils.crc32(content);
            if (current != crc32) {
                throw new TimelineException(TimelineExceptionType.INVALID_USE,
                        String.format("Message content is broken, expected crc32:%d, but:%d",
                        crc32, current));
            }
        }

        message.deserialize(content);
        message.setMessageID(messageID);
        return new TimelineEntry(sequenceID, message);
    }

    public static long crc32(byte[] content) {
        CRC32 crc32 = new CRC32();
        crc32.update(content, 0, content.length);
        return crc32.getValue();
    }

    public static String getLocalIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new TimelineException(TimelineExceptionType.ABORT,
                    "Can not get local machine ip.");
        }
    }

    public static String getProcessID() {
        String value = ManagementFactory.getRuntimeMXBean().getName();
        return value.substring(0, value.indexOf("@"));
    }
}
