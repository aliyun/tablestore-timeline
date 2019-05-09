package com.alicloud.openservices.tablestore.timeline2.query;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.alicloud.openservices.tablestore.model.filter.SingleColumnValueFilter;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestScanParameter {
    @Test
    public void testBasic() {
        Filter filter = new SingleColumnValueFilter("c0", SingleColumnValueFilter.CompareOperator.EQUAL, ColumnValue.fromString("b"));
        ScanParameter param = new ScanParameter()
                .maxCount(100)
                .scanForward(999)
                .withFilter(filter);

        assertEquals(param.getMaxCount(), 100);
        assertEquals(param.getFrom(), 999);
        assertEquals(param.getTo(), Long.MAX_VALUE);
        assertEquals(param.isForward(), true);
        assertEquals(param.getFilter(), filter);

        param = new ScanParameter()
                .scanBackward(999, 100);
        assertEquals(param.getFrom(), 999);
        assertEquals(param.getTo(), 100);
        assertEquals(param.isForward(), false);

        param = new ScanParameter()
                .scanForwardTo(100);
        assertEquals(param.getFrom(), 0);
        assertEquals(param.getTo(), 100);
        assertEquals(param.isForward(), true);

        param = new ScanParameter()
                .scanBackwardTo(100);
        assertEquals(param.getFrom(), Long.MAX_VALUE);
        assertEquals(param.getTo(), 100);
        assertEquals(param.isForward(), false);

        param = new ScanParameter()
                .scanBackward(100);
        assertEquals(param.getFrom(), 100);
        assertEquals(param.getTo(), 0);
        assertEquals(param.isForward(), false);
    }

    @Test
    public void testIllegalParam() {
        try {
            new ScanParameter().scanBackward(0, 100);
            fail();
        } catch (TimelineException e) {
        }

        try {
            new ScanParameter().scanForward(101, 100);
            fail();
        } catch (TimelineException e) {
        }
    }
}
