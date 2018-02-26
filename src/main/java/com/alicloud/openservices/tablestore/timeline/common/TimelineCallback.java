package com.alicloud.openservices.tablestore.timeline.common;


import com.alicloud.openservices.tablestore.timeline.TimelineEntry;

/**
 * 异步接口中的回调接口。
 * 如果LIB使用者选择异步时使用callback，则需要实现TimelineCallback接口。如果使用Future接口，则不需要实现TimelineCallback接口。
 * @param <T>   请求参数类型。
 */
public interface TimelineCallback<T> {
    /**
     * 请求成功后的回调函数。
     * @param timelineID        此次回调所属的Timeline的ID。
     * @param request           请求参数。
     * @param timelineEntry     返回的TimelineEntry对象。
     */
    public void onCompleted(final String timelineID, final T request, final TimelineEntry timelineEntry);

    /**
     * 请求失败后的回调函数。
     * @param timelineID        此次回调所属的Timeline的ID。
     * @param request           请求参数。
     * @param ex                失败后出现的异常对象。
     */
    public void onFailed(final String timelineID, final T request, final Exception ex);
}
