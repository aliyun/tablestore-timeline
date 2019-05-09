package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestIdentifierSchema {

    @Test
    public void testBasic() {
        TimelineIdentifierSchema schema = new TimelineIdentifierSchema.Builder()
                .addStringField("f0")
                .addLongField("f1")
                .addBinaryField("f2").build();

        List<PrimaryKeySchema> keys = schema.getKeys();
        assertEquals(keys.size(), 3);
        assertEquals(keys.get(0), new PrimaryKeySchema("f0", PrimaryKeyType.STRING));
        assertEquals(keys.get(1), new PrimaryKeySchema("f1", PrimaryKeyType.INTEGER));
        assertEquals(keys.get(2), new PrimaryKeySchema("f2", PrimaryKeyType.BINARY));
    }
}
