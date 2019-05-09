package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.search.query.BoolQuery;
import com.alicloud.openservices.tablestore.model.search.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FieldCondition implements Condition {
    private Query query;
    private FieldCondition(Query query) {
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }

    public static Field field(String name) {
        return new Field(name);
    }

    private static List<Query> toSubQueries(Condition... params) {
        List<Condition> conds = Arrays.asList(params);
        List<Query> subQueries = new ArrayList<Query>(conds.size());
        for (Condition cond : conds) {
            subQueries.add(cond.getQuery());
        }
        return subQueries;
    }

    public static FieldCondition and(Condition... params) {
        BoolQuery query = new BoolQuery();
        query.setMustQueries(toSubQueries(params));
        return new FieldCondition(query);
    }

    public static FieldCondition or(Condition... params) {
        BoolQuery query = new BoolQuery();
        query.setShouldQueries(toSubQueries(params));
        query.setMinimumShouldMatch(1);
        return new FieldCondition(query);
    }

    public static FieldCondition not(Condition... params) {
        BoolQuery query = new BoolQuery();
        query.setMustNotQueries(toSubQueries(params));
        return new FieldCondition(query);
    }
}
