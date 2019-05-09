package com.alicloud.openservices.tablestore.timeline2.utils;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestTimelineException {
    @Test
    public void testConvertException() {
        TableStoreException tableStoreException = new TableStoreException(
                "table not exist", new Exception(), "OTSObjectNotExist", "mockRequestId", 400
        );
        TimelineException e = Utils.convertException(tableStoreException);

        assertEquals("OTSObjectNotExist", e.getMessage());

        ClientException clientException = new ClientException();
        e = Utils.convertException(clientException);

        assertEquals("ClientError", e.getMessage());

        Exception exception = new Exception();
        e = Utils.convertException(exception);

        assertEquals("OtherError", e.getMessage());
    }
}
