package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class TestTimelineMessage {

    private TimelineMessage getMessage() {
        TimelineMessage message = new TimelineMessage();
        message.setField("c0", "hello world");
        message.setField("c1", true);
        message.setField("c2", 9.99);
        message.setField("c3", 20190418);
        message.setField("c4", new String[]{"a", "b", "c", "d", "e"});
        message.setField("c5", Arrays.asList("0", "1", "2", "3", "4"));
        message.setField(new Column("c6", ColumnValue.fromLong(9999)));
        return message;
    }

    @Test
    public void testBasic() {
        TimelineMessage message = getMessage();

        assertEquals(message.getFields().size(), 7);
        assertEquals(message.getString("c0"), "hello world");
        assertEquals(message.getBoolean("c1"), true);
        assertEquals(message.getDouble("c2"), 9.99, 0.001);
        assertEquals(message.getLong("c3"), 20190418);
        assertArrayEquals(message.getStringList("c4").toArray(), new String[]{"a", "b", "c", "d", "e"});
        assertArrayEquals(message.getStringList("c5").toArray(), new String[]{"0", "1", "2", "3", "4"});
        assertEquals(message.getLong("c6"), 9999);
        assertTrue(message.contains("c0"));
        assertTrue(message.contains("c1"));
        assertTrue(message.contains("c2"));
        assertTrue(message.contains("c3"));
        assertTrue(message.contains("c4"));
        assertTrue(message.contains("c5"));
        assertTrue(message.contains("c6"));
        assertTrue(!message.contains("c7"));

        assertEquals(message.getFields().size(), 7);

        assertEquals(message.toString(),
                "c0:hello world, c1:true, c2:9.99, c3:20190418, c4:[\"a\",\"b\",\"c\",\"d\",\"e\"], c5:[\"0\",\"1\",\"2\",\"3\",\"4\"], c6:9999");
    }

    @Test
    public void testTypeNotMatch() {
        TimelineMessage message = getMessage();

        try {
            message.getLong("c0");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            message.getString("c1");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            message.getDouble("c0");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            message.getBoolean("c2");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            message.getStringList("c0");
            fail();
        } catch (IllegalStateException e) {
        }
    }
}
