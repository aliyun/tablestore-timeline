package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.search.query.*;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;

public class Field implements Condition {
    private String name;
    private Query query;

    protected Field(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public Query getQuery() {
        return query;
    }

    private void checkQueryConflict (boolean allowRepeatSet, Class clazz) {
        Preconditions.checkArgument(
                query == null || (allowRepeatSet && clazz.isInstance(query)),
                "Condition set conflict.");
    }

    public Field equals(String value) {
        return equals(ColumnValue.fromString(value));
    }

    public Field equals(long value) {
        return equals(ColumnValue.fromLong(value));
    }

    public Field equals(double value) {
        return equals(ColumnValue.fromDouble(value));
    }

    public Field equals(boolean value) {
        return equals(ColumnValue.fromBoolean(value));
    }

    public Field in(String... values) {
        ColumnValue[] cvs = new ColumnValue[values.length];
        for (int i = 0; i < values.length; i++) {
            cvs[i] = ColumnValue.fromString(values[i]);
        }

        return in(cvs);
    }

    public Field in(long... values) {
        ColumnValue[] cvs = new ColumnValue[values.length];
        for (int i = 0; i < values.length; i++) {
            cvs[i] = ColumnValue.fromLong(values[i]);
        }

        return in(cvs);
    }

    public Field in(double... values) {
        ColumnValue[] cvs = new ColumnValue[values.length];
        for (int i = 0; i < values.length; i++) {
            cvs[i] = ColumnValue.fromDouble(values[i]);
        }

        return in(cvs);
    }

    private Field in(ColumnValue[] cvs) {
        checkQueryConflict(false, null);

        TermsQuery query = new TermsQuery();
        query.setFieldName(name);
        for (ColumnValue cv : cvs) {
            query.addTerm(cv);
        }

        this.query = query;
        return this;
    }

    private Field equals(ColumnValue value) {
        checkQueryConflict(false, null);

        TermQuery query = new TermQuery();
        query.setFieldName(name);
        query.setTerm(value);
        this.query = query;
        return this;
    }

    public Field match(String text) {
        checkQueryConflict(false, null);

        MatchQuery query = new MatchQuery();
        query.setFieldName(name);
        query.setText(text);
        this.query = query;
        return this;
    }

    public Field matchPhrase(String text) {
        checkQueryConflict(false, null);

        MatchPhraseQuery query = new MatchPhraseQuery();
        query.setFieldName(name);
        query.setText(text);
        this.query = query;
        return this;
    }

    public Field startsWith(String prefix) {
        checkQueryConflict(false, null);

        PrefixQuery query = new PrefixQuery();
        query.setFieldName(name);
        query.setPrefix(prefix);
        this.query = query;
        return this;
    }

    public Field matchWildcard(String wildcard) {
        checkQueryConflict(false, null);

        WildcardQuery query = new WildcardQuery();
        query.setFieldName(name);
        query.setValue(wildcard);
        this.query = query;
        return this;
    }

    private Field range(ColumnValue from, boolean fromSet, boolean includeFrom, ColumnValue to, boolean toSet, boolean includeTo) {
        RangeQuery query;
        if (this.query != null && this.query instanceof RangeQuery) {
            query = (RangeQuery)this.query;
        } else {
            checkQueryConflict(true, RangeQuery.class);
            query = new RangeQuery();
        }

        query.setFieldName(name);
        if (fromSet) {
            query.setFrom(from);
            query.setIncludeLower(includeFrom);
        }

        if (toSet) {
            query.setTo(to);
            query.setIncludeUpper(includeTo);
        }

        Preconditions.checkArgument(
                query.getFrom() == null || query.getTo() == null || query.getFrom().getType() == query.getTo().getType(),
                "The value of range boundary must be in the same type.");
        this.query = query;
        return this;
    }

    public Field greaterThan(long start) {
        return range(ColumnValue.fromLong(start), true, false, null, false, false);
    }

    public Field greaterEqual(long start) {
        return range(ColumnValue.fromLong(start), true, true, null, false, false);
    }

    public Field lessThan(long end) {
        return range(null, false, false, ColumnValue.fromLong(end), true, false);
    }

    public Field lessEqual(long end) {
        return range(null, false, false, ColumnValue.fromLong(end), true, true);
    }

    public Field greaterThan(String start) {
        return range(ColumnValue.fromString(start), true, false, null, false, false);
    }

    public Field greaterEqual(String start) {
        return range(ColumnValue.fromString(start), true, true, null, false, false);
    }

    public Field lessThan(String end) {
        return range(null, false, false, ColumnValue.fromString(end), true, false);
    }

    public Field lessEqual(String end) {
        return range(null, false, false, ColumnValue.fromString(end), true, true);
    }

    public Field greaterThan(double start) {
        return range(ColumnValue.fromDouble(start), true, false, null, false, false);
    }

    public Field greaterEqual(double start) {
        return range(ColumnValue.fromDouble(start), true, true, null, false, false);
    }

    public Field lessThan(double end) {
        return range(null, false, false, ColumnValue.fromDouble(end), true, false);
    }

    public Field lessEqual(double end) {
        return range(null, false, false, ColumnValue.fromDouble(end), true, true);
    }

    public Field withQuery(Query query) {
        this.query = query;
        return this;
    }
}
