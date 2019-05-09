package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.search.sort.FieldSort;
import com.alicloud.openservices.tablestore.model.search.sort.Sort;
import com.alicloud.openservices.tablestore.model.search.sort.SortOrder;
import com.alicloud.openservices.tablestore.timeline2.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class SearchParameter {
    private Condition fieldCondition;
    private int offset = 0;
    private int limit = 100;
    private Sort sort;
    private byte[] token;
    private boolean calculateTotalCount = false;

    public SearchParameter(Condition fieldCondition) {
        this.fieldCondition = fieldCondition;
    }

    public SearchParameter offset(int offset) {
        this.offset = offset;
        return this;
    }

    public SearchParameter limit(int limit) {
        this.limit = limit;
        return this;
    }

    public SearchParameter withToken(byte[] token) {
        this.token = token;
        this.offset = 0;//offset must be set 0 or -1 when token is set
        return this;
    }

    public SearchParameter calculateTotalCount() {
        this.calculateTotalCount = true;
        return this;
    }

    public SearchParameter orderBy(String[] fields, SortOrder order) {
        Preconditions.checkArgument(sort == null, "You have already set sort condition.");

        List<Sort.Sorter> sorters = new ArrayList<Sort.Sorter>(fields.length);
        for (String field : fields) {
            sorters.add(new FieldSort(field, order));
        }
        sort = new Sort(sorters);
        return this;
    }

    public Condition getFieldCondition() {
        return fieldCondition;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public Sort getSort() {
        return sort;
    }

    public boolean isCalculateTotalCount() {
        return calculateTotalCount;
    }

    public byte[] getToken() {
        return token;
    }
}
