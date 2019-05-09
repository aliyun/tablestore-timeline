package com.alicloud.openservices.tablestore.timeline2.model;

import com.alicloud.openservices.tablestore.model.search.FieldSchema;
import com.alicloud.openservices.tablestore.model.search.FieldType;
import com.alicloud.openservices.tablestore.model.search.IndexSchema;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestTimelineMetaSchema {

    @Test
    public void testBasic() {
        String tableName = "table1";
        TimelineIdentifierSchema identifierSchema = new TimelineIdentifierSchema.Builder()
                .addStringField("pk0")
                .addStringField("pk1").build();
        TimelineMetaSchema schema = new TimelineMetaSchema(tableName, identifierSchema);
        assertEquals(schema.getTableName(), tableName);
        assertArrayEquals(schema.getIdentifierSchema().getKeys().toArray(), identifierSchema.getKeys().toArray());
        assertEquals(schema.hasMetaIndex(), false);

        String indexName = "index1";
        IndexSchema indexSchema = new IndexSchema();
        indexSchema.addFieldSchema(new FieldSchema("pk0", FieldType.KEYWORD).setIndex(true).setEnableSortAndAgg(true));
        indexSchema.addFieldSchema(new FieldSchema("pk1", FieldType.TEXT).setIndex(true));
        schema.withIndex(indexName, indexSchema);

        assertEquals(schema.getIndexName(), indexName);
        assertEquals(schema.getIndexSchema(), indexSchema);
    }
}
