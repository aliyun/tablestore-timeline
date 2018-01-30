package com.alicloud.openservices.tablestore.timeline.store;

import com.alicloud.openservices.tablestore.AsyncClient;
import com.alicloud.openservices.tablestore.model.RangeIteratorParameter;
import com.alicloud.openservices.tablestore.model.RowIterator;
import com.alicloud.openservices.tablestore.timeline.TimelineEntry;
import com.alicloud.openservices.tablestore.timeline.utils.Utils;

import java.util.Iterator;

/**
 * DistributeTimeline的迭代器，用于逐个遍历读取消息。
 */
public class DistributeTimelineIterator implements Iterator<TimelineEntry>{
    private RowIterator rowIterator = null;
    private DistributeTimelineConfig config = null;

    DistributeTimelineIterator(AsyncClient client, RangeIteratorParameter iteratorParameter, DistributeTimelineConfig config) {
        rowIterator = new RowIterator(client.asSyncClient(), iteratorParameter);
        this.config = config;
    }

    /**
     * 判断是否还有下一条有效TimelineEntry。
     * @return  true/false
     */
    @Override
    public boolean hasNext() {
        return rowIterator.hasNext();
    }

    /**
     * 获取下一条TimelineEntry。
     * 这里可以拿到每条消息的消息ID，但是没法做去重。
     * 原因是：这里只能去重每次Scan返回的结果中的数据，没法跨Scan去重，如果Scan的某条结果和前一次Scan的某条结果消息ID重复，在这里是没法发现的。
     * 最好的去重方式，需要Timeline LIB的用户在读取到结果后做一次全局去重（Timeline域）。
     * @return      下一条TimelineEntry。
     */
    @Override
    public TimelineEntry next() {
        return Utils.toTimelineEntry(rowIterator.next(), this.config);
    }

    /**
     * 删除当前TimelineEntry。
     * 当前不支持，会抛出异常。
     */
    @Override
    public void remove() {
        rowIterator.remove();
    }
}
