package com.alicloud.openservices.tablestore.timeline;

import com.alicloud.openservices.tablestore.timeline.common.TimelineException;
import com.alicloud.openservices.tablestore.timeline.message.IMessage;
import com.alicloud.openservices.tablestore.timeline.message.StringMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    @Test
    public void testUpdateAttributeFailed() {
        String content = "hangzhou";
        IMessage message = new StringMessage(content);
        try {
            message.updateAttribute("name", "hangzhou");
            fail();
        } catch (TimelineException ex) {
        }
    }

    @Test
    public void testUpdateAttribute() {
        String content = "hangzhou";
        IMessage message = new StringMessage(content);
        message.addAttribute("name", "hangzhou");

        assertEquals("hangzhou", message.getAttributes().get("name"));

        message.updateAttribute("name", "nanjing");
        assertEquals("nanjing", message.getAttributes().get("name"));
    }
}
