package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTimelineIdentifier {

    @Test
    public void testBasic() {
        TimelineIdentifier identifier = new TimelineIdentifier.Builder()
                .addField("f0", "test")
                .addField("f1", 20190418)
                .addField("f2", new byte[]{0x10, 0x11, 0x12})
                .addField(new PrimaryKeyColumn("f3", PrimaryKeyValue.fromLong(10000L))).build();

        TimelineIdentifier identifier2 = new TimelineIdentifier.Builder()
                .addField("f0", "test")
                .addField("f1", 20190418)
                .addField("f2", new byte[]{0x10, 0x11, 0x12})
                .addField(new PrimaryKeyColumn("f3", PrimaryKeyValue.fromLong(10000L))).build();

        TimelineIdentifier identifier3 = new TimelineIdentifier.Builder()
                .addField("f0", "test")
                .addField("f1", 20190418)
                .addField("f2", new byte[]{0x10, 0x11, 0x13})
                .addField(new PrimaryKeyColumn("f3", PrimaryKeyValue.fromLong(10000L))).build();

        assertEquals(identifier, identifier2);
        assertTrue(!identifier.equals(identifier3));
        assertEquals(identifier.hashCode(), identifier2.hashCode());

        assertEquals(identifier.getField(0).getName(), "f0");
        assertEquals(identifier.getField(0).getValue().asString(), "test");
        assertEquals(identifier.getField(1).getName(), "f1");
        assertEquals(identifier.getField(1).getValue().asLong(), 20190418);
        assertEquals(identifier.getField(2).getName(), "f2");
        assertArrayEquals(identifier.getField(2).getValue().asBinary(), new byte[]{0x10, 0x11, 0x12});
        assertEquals(identifier.getField(3).getName(), "f3");
        assertEquals(identifier.getField(3).getValue().asLong(), 10000);

        assertEquals(identifier.getFields().size(), 4);
    }
}
