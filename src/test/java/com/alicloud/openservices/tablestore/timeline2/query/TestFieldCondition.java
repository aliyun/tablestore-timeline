package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import org.junit.Test;

import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TestFieldCondition {
    @Test
    public void testBasic() {
        Field f0 = field("f0").equals("a");
        Field f1 = field("f1").equals("b");
        FieldCondition c0 = and(f0, f1);
        BoolQuery query = (BoolQuery)c0.getQuery();
        assertEquals(query.getMustQueries().size(), 2);
        assertNull(query.getShouldQueries());
        assertNull(query.getMustNotQueries());
        assertNull(query.getMinimumShouldMatch());

        FieldCondition c1 = or(f0, f1);
        query = (BoolQuery)c1.getQuery();
        assertEquals(query.getShouldQueries().size(), 2);
        assertNull(query.getMustNotQueries());
        assertNull(query.getMustNotQueries());
        assertEquals(query.getMinimumShouldMatch().intValue(), 1);

        FieldCondition c2 = not(f0, f1);
        query = (BoolQuery)c2.getQuery();
        assertEquals(query.getMustNotQueries().size(), 2);
        assertNull(query.getMustQueries());
        assertNull(query.getShouldQueries());
        assertNull(query.getMinimumShouldMatch());

        FieldCondition c3 = and(c0, or(c1, c2));
        query = (BoolQuery)c3.getQuery();
        assertEquals(query.getMustQueries().size(), 2);
        assertEquals(query.getMustQueries().get(0), c0.getQuery());
        assertEquals(((BoolQuery)query.getMustQueries().get(1)).getShouldQueries().get(0), c1.getQuery());
        assertEquals(((BoolQuery)query.getMustQueries().get(1)).getShouldQueries().get(1), c2.getQuery());
    }
}
