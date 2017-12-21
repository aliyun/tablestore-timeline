package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.CRC32;

class Utils {
    final static int CONTENT_COLUMN_START_ID = 10000;

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    static <Res> Res waitForFuture(Future<Res> f) {
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

    static TimelineEntry toTimelineEntry(PutRowResponse response, IMessage message) {
        long sequenceID = response.getRow().getPrimaryKey().getPrimaryKeyColumn(1).getValue().asLong();
        return new TimelineEntry(sequenceID, message);
    }

    static TimelineEntry toTimelineEntry(Row row, DistributeTimelineConfig config) {
        PrimaryKey pk = row.getPrimaryKey();
        int pkCount = pk.getPrimaryKeyColumns().length;
        if (pkCount != 2) {
            throw new TimelineException(TimelineExceptionType.TET_INVALID_USE,
                    "Invalid Primary Key column count, expected:2, but:" + String.valueOf(pkCount));
        }

        Long sequenceID = pk.getPrimaryKeyColumn(1).getValue().asLong();

        Column[] columns = row.getColumns();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        String messageID = null;
        int index = CONTENT_COLUMN_START_ID;
        long crc32 = 0;
        for (Column column: columns) {
            String name = column.getName();
            if (name.startsWith(config.getMessageContentPrefix())) {
                int columnSeqID = Integer.parseInt(name.substring(config.getMessageContentPrefix().length()));
                if (columnSeqID != index) {
                    throw new TimelineException(TimelineExceptionType.TET_INVALID_USE,
                            String.format("Message Content column sequence id is wrong, expected:%d, but:%d",
                            index, columnSeqID));
                }
                index += 1;

                byte[] value = column.getValue().asBinary();
                stream.write(value, 0, value.length);
            } else if (name.equals(config.getMessageIDColumnName())) {
                messageID = column.getValue().asString();
            } else if (name.equals(config.getColumnNameOfMessageCrc32())) {
                crc32 = column.getValue().asLong();
            }
        }
        byte[] content = stream.toByteArray();

        try {
            stream.close();
        } catch (IOException ex) {
            logger.error("Close ByteArrayOutputStream failed", ex);
            throw new TimelineException(TimelineExceptionType.TET_ABORT,
                    "Close ByteArrayOutputStream failed", ex);
        }

        if (config.getColumnNameOfMessageCrc32() != null && !config.getColumnNameOfMessageCrc32().isEmpty()) {
            long current = Utils.crc32(content);
            if (current != crc32) {
                throw new TimelineException(TimelineExceptionType.TET_INVALID_USE,
                        String.format("Message content is broken, expected crc32:%d, but:%d",
                        crc32, current));
            }
        }

        IMessage message = config.getMessageInstance().newInstance();
        message.deserialize(content);
        message.setMessageID(messageID);
        return new TimelineEntry(sequenceID, message);
    }

    static long crc32(byte[] content) {
        CRC32 crc32 = new CRC32();
        crc32.update(content, 0, content.length);
        return crc32.getValue();
    }

    static String getLocalIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new TimelineException(TimelineExceptionType.TET_ABORT,
                    "Can not get local machine ip.");
        }
    }

    static String getProcessID() {
        String value = ManagementFactory.getRuntimeMXBean().getName();
        return value.substring(0, value.indexOf("@"));
    }
}
