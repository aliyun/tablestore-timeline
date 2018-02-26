package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.model.filter.Filter;

/**
 * 范围读取的参数类，涉及到：范围读取方向、起始位置、结束位置和最多返回个数。
 */
public class ScanParameter {
    /**
     * 范围读取的起始位置，值是顺序ID。
     */
    private Long from = null;

    /**
     * 范围读取的结束位置，
     */
    private Long to = null;

    /**
     * 每次范围读取最大返回个数。
     */
    private Integer maxCount = null;

    /**
     * 读取方向，是否是正序，支持：正序，逆序。
     */
    private boolean isForward = true;

    /*
     * 过滤条件。
     */
    private Filter filter = null;

    /**
     * ScanParameter构造函数，需要设置方向。
     * @param isForward     是否是正序。
     */
    ScanParameter(boolean isForward) {
        this.isForward = isForward;
    }

    /**
     * 获取范围扫描的起始位置，package内可见，用户不会使用此接口。
     * @return  其实位置的顺序ID。
     */
    public Long getFrom() {
        return from;
    }

    /**
     * 设置范围扫描的起始位置，package内可见，用户不会使用此接口。
     * @param from  起始位置的顺序ID。
     */
    public void setFrom(Long from) {
        this.from = from;
    }

    /**
     * 获取范围扫描的结束位置，package内可见，用户不会使用此接口。
     * @return      结束位置的顺序ID。
     */
    public Long getTo() {
        return to;
    }

    /**
     * 设置范围扫描的结束位置，package内可见，用户不会使用此接口。
     * @param to    结束位置的顺序ID。
     */
    public void setTo(Long to) {
        this.to = to;
    }

    /**
     * 获取最大返回个数，package内可见，用户不会使用此接口。。
     * @return      最大返回个数。
     */
    public Integer getMaxCount() {
        return maxCount;
    }

    /**
     * 设置最大返回个数，package内可见，用户不会使用此接口。
     * @param maxCount  最大返回个数。
     */
    public void setMaxCount(Integer maxCount) {
        this.maxCount = maxCount;
    }

    /**
     * 范围扫描的顺序是否是正向。
     * @return  正向返回true，逆向返回false。
     */
    public boolean isForward() { return isForward; }

    /**
     * 设置filter过滤条件。
     * @return
     */
    public Filter getFilter() {
        return filter;
    }

    /**
     * 获取过滤条件。
     * @param filter
     */
    public void setFilter(Filter filter) {
        this.filter = filter;
    }
}
