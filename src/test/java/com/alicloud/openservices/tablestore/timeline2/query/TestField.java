package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import org.junit.Test;

import static com.alicloud.openservices.tablestore.timeline2.query.FieldCondition.field;
import static org.junit.Assert.*;

public class TestField {
    @Test
    public void testEquals() {
        // string field
        Field f = field("f0").equals("a");
        Query q = f.getQuery();
        assertEquals(f.getName(), "f0");
        TermQuery tq = (TermQuery)q;
        assertEquals(tq.getFieldName(), "f0");
        assertEquals(tq.getTerm(), ColumnValue.fromString("a"));

        f = field("f1").equals(100L);
        assertEquals("f1", f.getName());
        tq = (TermQuery)f.getQuery();
        assertEquals(tq.getFieldName(), f.getName());
        assertEquals(tq.getTerm(), ColumnValue.fromLong(100L));

        f = field("f2").equals(9.99);
        assertEquals("f2", f.getName());
        tq = (TermQuery)f.getQuery();
        assertEquals(tq.getFieldName(), f.getName());
        assertEquals(tq.getTerm(), ColumnValue.fromDouble(9.99));


        f = field("f3").equals(false);
        assertEquals("f3", f.getName());
        tq = (TermQuery)f.getQuery();
        assertEquals(tq.getFieldName(), f.getName());
        assertEquals(tq.getTerm(), ColumnValue.fromBoolean(false));

        checkRepeateCondition(f);
    }

    private void checkRepeateCondition(Field f) {
        try {
            f.equals("a");
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.equals(100L);
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.equals(9.99);
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.equals(false);
            fail();
        } catch (TimelineException e) {
        }


        try {
            f.in(0, 2, 3);
            fail();
        } catch (TimelineException e) {
        }


        try {
            f.in("a", "b", "c");
            fail();
        } catch (TimelineException e) {
        }


        try {
            f.in(0.01, 0.02, 0.03);
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.match("h");
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.matchPhrase("h");
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.startsWith("p");
            fail();
        } catch (TimelineException e) {
        }

        try {
            f.matchWildcard("h*");
            fail();
        } catch (TimelineException e) {
        }
    }

    @Test
    public void testIn() {
        Field f = field("f0").in(0, 10, 100, 1000, 10000);
        assertEquals(f.getName(), "f0");
        TermsQuery q = (TermsQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertArrayEquals(q.getTerms().toArray(), new ColumnValue[]{ColumnValue.fromLong(0), ColumnValue.fromLong(10),
            ColumnValue.fromLong(100), ColumnValue.fromLong(1000), ColumnValue.fromLong(10000)});

        f = field("f1").in("a", "b", "c", "d", "e");
        assertEquals(f.getName(), "f1");
        q = (TermsQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertArrayEquals(q.getTerms().toArray(), new ColumnValue[]{ColumnValue.fromString("a"), ColumnValue.fromString("b"),
                ColumnValue.fromString("c"), ColumnValue.fromString("d"), ColumnValue.fromString("e")});

        f = field("f2").in(0.9, 0.2, 0.1, 0.05, 9.1);
        assertEquals(f.getName(), "f2");
        q = (TermsQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertArrayEquals(q.getTerms().toArray(), new ColumnValue[]{ColumnValue.fromDouble(0.9), ColumnValue.fromDouble(0.2),
                ColumnValue.fromDouble(0.1), ColumnValue.fromDouble(0.05), ColumnValue.fromDouble(9.1)});

        checkRepeateCondition(f);
    }

    @Test
    public void testMatch() {
        Field f = field("f0").match("hello world");
        assertEquals(f.getName(), "f0");
        MatchQuery q = (MatchQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertNull(q.getMinimumShouldMatch());
        assertEquals(q.getText(), "hello world");
        checkRepeateCondition(f);
    }

    @Test
    public void testMatchPhrase() {
        Field f = field("f0").matchPhrase("hello world");
        assertEquals(f.getName(), "f0");
        MatchPhraseQuery q = (MatchPhraseQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertEquals(q.getText(), "hello world");
        checkRepeateCondition(f);
    }

    @Test
    public void testStartsWith() {
        Field f = field("f0").startsWith("prefix");
        assertEquals(f.getName(), "f0");
        PrefixQuery q = (PrefixQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertEquals(q.getPrefix(), "prefix");
        checkRepeateCondition(f);
    }

    @Test
    public void testWildcard() {
        Field f = field("f0").matchWildcard("hhh*");
        assertEquals(f.getName(), "f0");
        WildcardQuery q = (WildcardQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertEquals(q.getValue(), "hhh*");
        checkRepeateCondition(f);
    }

    @Test
    public void testStringRange() {
        Field f = field("f0").greaterThan("a");
        assertEquals(f.getName(), "f0");
        RangeQuery q = (RangeQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertEquals(q.getFrom(), ColumnValue.fromString("a"));
        assertEquals(q.isIncludeLower(), false);
        assertNull(q.getTo());

        f.lessThan("b");
        assertEquals(q.getFrom(), ColumnValue.fromString("a"));
        assertEquals(q.isIncludeLower(), false);
        assertEquals(q.getTo(), ColumnValue.fromString("b"));
        assertEquals(q.isIncludeUpper(), false);

        f.greaterEqual("c");
        assertEquals(q.getFrom(), ColumnValue.fromString("c"));
        assertEquals(q.isIncludeLower(), true);

        f.lessEqual("d");
        assertEquals(q.getTo(), ColumnValue.fromString("d"));
        assertEquals(q.isIncludeUpper(), true);
    }

    @Test
    public void testLongRange() {
        Field f = field("f0").greaterThan(1);
        assertEquals(f.getName(), "f0");
        RangeQuery q = (RangeQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertEquals(q.getFrom(), ColumnValue.fromLong(1));
        assertEquals(q.isIncludeLower(), false);
        assertNull(q.getTo());

        f.lessThan(3);
        assertEquals(q.getFrom(), ColumnValue.fromLong(1));
        assertEquals(q.isIncludeLower(), false);
        assertEquals(q.getTo(), ColumnValue.fromLong(3));
        assertEquals(q.isIncludeUpper(), false);

        f.greaterEqual(2);
        assertEquals(q.getFrom(), ColumnValue.fromLong(2));
        assertEquals(q.isIncludeLower(), true);

        f.lessEqual(10);
        assertEquals(q.getTo(), ColumnValue.fromLong(10));
        assertEquals(q.isIncludeUpper(), true);
    }

    @Test
    public void testDoubleRange() {
        Field f = field("f0").greaterThan(0.1);
        assertEquals(f.getName(), "f0");
        RangeQuery q = (RangeQuery)f.getQuery();
        assertEquals(q.getFieldName(), f.getName());
        assertEquals(q.getFrom(), ColumnValue.fromDouble(0.1));
        assertEquals(q.isIncludeLower(), false);
        assertNull(q.getTo());

        f.lessThan(10.1);
        assertEquals(q.getFrom(), ColumnValue.fromDouble(0.1));
        assertEquals(q.isIncludeLower(), false);
        assertEquals(q.getTo(), ColumnValue.fromDouble(10.1));
        assertEquals(q.isIncludeUpper(), false);

        f.greaterEqual(5.5);
        assertEquals(q.getFrom(), ColumnValue.fromDouble(5.5));
        assertEquals(q.isIncludeLower(), true);

        f.lessEqual(9.9);
        assertEquals(q.getTo(), ColumnValue.fromDouble(9.9));
        assertEquals(q.isIncludeUpper(), true);
    }

    @Test
    public void testWithQuery() {
        GeoDistanceQuery q = new GeoDistanceQuery();
        q.setFieldName("f0");
        q.setDistanceInMeter(10.0);
        Field f = field("f0").withQuery(q);
        assertEquals(f.getQuery(), q);
    }

    @Test
    public void testRangeInvalid() {
        Field f = field("f1").equals("a");
        try {
            f.lessThan("a");
            fail();
        } catch (TimelineException e) {
        }

        try {
            field("f1").lessThan("a").greaterThan(1);
            fail();
        } catch (TimelineException e) {
        }
    }
}
