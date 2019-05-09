package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class TestTimelineMeta {
    private TimelineMeta getMeta() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("p0", "key").build();
        TimelineMeta meta = new TimelineMeta(identifier);
        meta.setField("c0", "hello world");
        meta.setField("c1", true);
        meta.setField("c2", 9.99);
        meta.setField("c3", 20190418);
        meta.setField("c4", new String[]{"a", "b", "c", "d", "e"});
        meta.setField("c5", Arrays.asList("0", "1", "2", "3", "4"));
        meta.setField(new Column("c6", ColumnValue.fromLong(9999)));
        return meta;
    }

    @Test
    public void testBasic() {
        TimelineMeta meta = getMeta();

        assertEquals(meta.getFields().size(), 7);
        assertEquals(meta.getString("c0"), "hello world");
        assertEquals(meta.getBoolean("c1"), true);
        assertEquals(meta.getDouble("c2"), 9.99, 0.001);
        assertEquals(meta.getLong("c3"), 20190418);
        assertArrayEquals(meta.getStringList("c4").toArray(), new String[]{"a", "b", "c", "d", "e"});
        assertArrayEquals(meta.getStringList("c5").toArray(), new String[]{"0", "1", "2", "3", "4"});
        assertEquals(meta.getLong("c6"), 9999);
        assertTrue(meta.contains("c0"));
        assertTrue(meta.contains("c1"));
        assertTrue(meta.contains("c2"));
        assertTrue(meta.contains("c3"));
        assertTrue(meta.contains("c4"));
        assertTrue(meta.contains("c5"));
        assertTrue(meta.contains("c6"));
        assertTrue(!meta.contains("c7"));

        assertEquals(meta.getFields().size(), 7);

        assertEquals(meta.toString(),
                "Identifier: ['p0':key], Columns: [c0:hello world, c1:true, c2:9.99, c3:20190418, c4:[\"a\",\"b\",\"c\",\"d\",\"e\"], c5:[\"0\",\"1\",\"2\",\"3\",\"4\"], c6:9999]");

        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("p0", "key").build();
        assertEquals(identifier, meta.getIdentifier());
    }

    @Test
    public void testTypeNotMatch() {
        TimelineMeta meta = getMeta();

        try {
            meta.getLong("c0");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            meta.getString("c1");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            meta.getDouble("c0");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            meta.getBoolean("c2");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            meta.getStringList("c0");
            fail();
        } catch (IllegalStateException e) {
        }
    }
}
