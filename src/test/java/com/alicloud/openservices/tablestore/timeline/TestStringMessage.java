package com.alicloud.openservices.tablestore.timeline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestStringMessage {
    @Test
    public void testSerializeWithEmptyContent() {
        IMessage message = new StringMessage();
        byte[] bytes = message.serialize();
        assertEquals(0, bytes.length);

        StringMessage message2 = new StringMessage();
        message2.deserialize(bytes);
        assertEquals(0, message2.getContent().length());
    }

    @Test
    public void testNoDeserialize() {
        StringMessage message2 = new StringMessage();
        assertEquals(0, message2.getContent().length());
    }

    @Test
    public void testSerializeAndDeserialize() {
        String content = "hangzhou";
        IMessage message = new StringMessage(content);
        byte[] bytes = message.serialize();
        assertTrue(bytes.length > 0);

        StringMessage message2 = new StringMessage();
        message2.deserialize(bytes);
        assertEquals(content, message2.getContent());
    }
}
