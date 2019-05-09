package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.timeline2.utils.Utils;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

public class TimelineMessageForV1 {
    private String stringMessage = null;
    private TimelineMessage message = new TimelineMessage();

    public final static int CONTENT_COLUMN_START_ID = 10000;
    public final static String SYSTEM_COLUMN_NAME_PREFIX = "__";
    private static AtomicInteger baseID = new AtomicInteger(0);
    private static String machineID = Utils.getProcessID() + "@" + Utils.getLocalIP() + ":";

    /**
     * The column name of message id.
     */
    private String messageIDColumnNameSuffix = "message_id";

    /**
     * the prefix of message content.
     */
    private String messageContentSuffix = "content";
    private String messageContentCountSuffix = "column_count";

    /**
     * check message crc32 if it's complete.
     */
    private String columnNameOfMessageCrc32Suffix = "crc32";

    /**
     * The max length of column (Binary type), default 1MB.
     */
    private int columnMaxLength = 1024 * 1024;


    public TimelineMessageForV1(String content) {
        baseID.compareAndSet(Integer.MAX_VALUE, 0);
        String messageID = machineID + String.valueOf(baseID.addAndGet(1));

        message.setField(SYSTEM_COLUMN_NAME_PREFIX + messageIDColumnNameSuffix, messageID);
        setContent(content);
    }

    public TimelineMessageForV1(TimelineMessage message) {
        this.message = message;
    }


    public TimelineMessage getTimelineMessage() {
        return message;
    }

    public TimelineMessageForV1 setMessageId(String messageID) {
        message.setField(SYSTEM_COLUMN_NAME_PREFIX + messageIDColumnNameSuffix, messageID);
        return this;
    }

    public String getMessageID() {
        return message.getString(SYSTEM_COLUMN_NAME_PREFIX + messageIDColumnNameSuffix);
    }

    public TimelineMessageForV1 setContent(String stringMessage) {
        byte[] content = stringMessage.getBytes();//编码
        if (content.length > 2 * 1024 * 1024) {
            throw new TimelineException(String.format("Message Content must less than 2MB, current:%s", String.valueOf(content.length)));
        }

        /**
         * Write message content.
         */
        int pos = 0;
        int index = CONTENT_COLUMN_START_ID;
        while (pos < content.length) {
            byte[] columnValue;
            if (pos + columnMaxLength < content.length) {
                columnValue = Arrays.copyOfRange(content, pos, pos + columnMaxLength);
            } else {
                columnValue = Arrays.copyOfRange(content, pos, content.length);
            }
            String columnName = SYSTEM_COLUMN_NAME_PREFIX + messageContentSuffix + String.valueOf(index++);
            message.setField(new Column(columnName, ColumnValue.fromBinary(columnValue)));
            pos += columnValue.length;
        }

        /**
         * Write Message Content Count
         */
        {
            String columnName = SYSTEM_COLUMN_NAME_PREFIX + messageContentCountSuffix;
            message.setField(columnName, index - CONTENT_COLUMN_START_ID);
        }

        /**
         * Write CRC32.
         */
        if (columnNameOfMessageCrc32Suffix != null && !columnNameOfMessageCrc32Suffix.isEmpty()) {
            long crc32 = crc32(content);
            String columnName = SYSTEM_COLUMN_NAME_PREFIX + columnNameOfMessageCrc32Suffix;
            message.setField(columnName, crc32);
        }

        return this;
    }

    public String getContent() {
        if (stringMessage != null) {
            return stringMessage;
        }

        /**
         * Get Message Content Count
         */
        String contentCountName = SYSTEM_COLUMN_NAME_PREFIX + messageContentCountSuffix;
        long currentCount = 0;
        long columnCount = message.getLong(contentCountName);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        /**
         * Get CRC32 And Message Content..
         */
        int index = CONTENT_COLUMN_START_ID;
        long crc32 = 0;

        Map<String, Column> columns = message.getFields();
        List<Map.Entry<String,Column>> list = new ArrayList<Map.Entry<String,Column>>(columns.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String,Column>>(){
            @Override
            public int compare(Map.Entry<String, Column> o1, Map.Entry<String, Column> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        for (Map.Entry<String,Column> entry: list) {
            String key = entry.getKey();
            Column column = columns.get(key);
            String name = column.getName();

            if (name.startsWith(SYSTEM_COLUMN_NAME_PREFIX + messageContentSuffix)) {
                if (currentCount == columnCount) {
                    continue;
                }
                int columnSeqID = Integer.parseInt(name.substring(SYSTEM_COLUMN_NAME_PREFIX.length() + messageContentSuffix.length()));
                if (columnSeqID != index) {
                    throw new TimelineException(String.format("Message Content column sequence id is wrong, expected:%d, but:%d", index, columnSeqID));
                }

                byte[] value = column.getValue().asBinary();
                stream.write(value, 0, value.length);

                index++;
                currentCount++;
            } else if (name.equals(SYSTEM_COLUMN_NAME_PREFIX + columnNameOfMessageCrc32Suffix)) {
                crc32 = column.getValue().asLong();
            }
        }

        if (columnCount != -1 && currentCount != columnCount) {
            throw new TimelineException(String.format("Message content column is broken, expected %d, but %d", columnCount, currentCount));
        }
        byte[] content = stream.toByteArray();

        try {
            stream.close();
        } catch (IOException ex) {
            throw new TimelineException("Close ByteArrayOutputStream failed", ex);
        }

        /**
         * Check CRC32
         * */
        if (columnNameOfMessageCrc32Suffix != null && !columnNameOfMessageCrc32Suffix.isEmpty()) {
            long current = crc32(content);
            if (current != crc32) {
                throw new TimelineException(String.format("Message content is broken, expected crc32:%d, but:%d", crc32, current));
            }
        }

        stringMessage = new String(content);

        return stringMessage;
    }

    public void addAttribute(String name, String value) {
        message.setField(name, value);
    }

    public String getAttribute(String name) {
        return message.getString(name);
    }

    public static long crc32(byte[] content) {
        CRC32 crc32 = new CRC32();
        crc32.update(content, 0, content.length);
        return crc32.getValue();
    }
}
