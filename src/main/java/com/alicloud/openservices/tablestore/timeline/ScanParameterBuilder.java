package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.common.TimelineExceptionType;

/**
 * ScanParameter类的构造类，LIB使用者必须使用ScanParameterBuilder构造ScanParameter。
 */
public class ScanParameterBuilder {
    private ScanParameter parameter = null;

    /**
     * 构造函数，私有接口，用户不能操作此接口，请通过scanForward或scanBackward接口构造对象。
     * @param parameter     ScanParameter对象。
     */
    private ScanParameterBuilder(ScanParameter parameter) {
        this.parameter = parameter;
    }

    /**
     * 静态方法，构造一个正序ScanParameter对象。
     * 正序的场景，包括但不限于：IM消息的同步读取。
     * @return      正序的ScanParameter对象。
     */
    public static ScanParameterBuilder scanForward() {
        ScanParameter parameter = new ScanParameter(true);
        return new ScanParameterBuilder(parameter);
    }

    /**
     * 静态方法，构造一个逆序ScanParameter对象。
     * 逆序的场景：包括但不限于：IM消息的历史回话读取；微博/朋友圈等Feed流场景的最新Feed读取等。
     * @return  逆序的ScanParameter对象。
     */
    public static ScanParameterBuilder scanBackward() {
        ScanParameter parameter = new ScanParameter(false);
        return new ScanParameterBuilder(parameter);
    }

    /**
     * 设置ScanParameter对象中的起始位置。
     * @param sequenceID    起始位置的顺序ID。
     * @return              ScanParameterBuilder对象，用于串行调用剩余参数接口。
     */
    public ScanParameterBuilder from(long sequenceID) {
        if (sequenceID < 0) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "from must more than or equal 0");
        }

        this.parameter.setFrom(sequenceID);
        return this;
    }

    /**
     * 设置ScanParameter对象中的结束位置。
     * @param sequenceID    结束位置的顺序ID。
     * @return              ScanParameterBuilder对象，用于串行调用剩余参数接口。
     */
    public ScanParameterBuilder to(long sequenceID) {
        if (sequenceID < 0) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "to must more than or equal 0");
        }

        this.parameter.setTo(sequenceID);
        return this;
    }

    /**
     * 设置ScanParameter对象中的最大返回个数。
     * @param maxCount      最大返回个数
     * @return              ScanParameterBuilder对象，用于串行调用剩余参数接口。
     */
    public ScanParameterBuilder maxCount(int maxCount) {
        if (maxCount < 0) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "maxCount must more than or equal 0");
        }

        this.parameter.setMaxCount(maxCount);
        return this;
    }

    public ScanParameterBuilder filter(Filter filter) {
        if (filter == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "filter is null");
        }
        this.parameter.setFilter(filter);
        return this;
    }

    /**
     * 生成ScanParameter对象，需要调用完from，to和maxCount后再调用build接口。
     * @return      ScanParameter对象。
     * @throws      TimelineException 异常
     */
    public ScanParameter build() {
        if (parameter.getFrom() == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "ScanParameter's 'from' parameter is Null");
        }

        if (parameter.getTo() == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "ScanParameter's 'to' parameter is Null");
        }

        if (parameter.getMaxCount() == null) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "ScanParameter's 'maxCount' parameter is Null");
        }

        if (parameter.isForward() && parameter.getFrom() > parameter.getTo()) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "from must less than to in FORWARD");
        }

        if (!parameter.isForward() && parameter.getFrom() < parameter.getTo()) {
            throw new TimelineException(TimelineExceptionType.INVALID_USE,
                    "from must more than to in BACKWARD");
        }

        return parameter;
    }
}
