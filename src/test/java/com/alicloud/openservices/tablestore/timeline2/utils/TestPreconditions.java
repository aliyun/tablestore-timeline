package com.alicloud.openservices.tablestore.timeline2.utils;

import com.alicloud.openservices.tablestore.timeline2.TimelineException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestPreconditions {
    @Test
    public void testCheckNotNull() {

        try {
            Preconditions.checkNotNull(null);
            fail();
        } catch (TimelineException e) {
        }

        try {
            Preconditions.checkNotNull(null, "not null");
            fail();
        } catch (TimelineException e) {
            assertEquals("not null", e.getMessage());
        }

        try {
            Preconditions.checkNotNull(null, "%s-%s", "not", "null");
            fail();
        } catch (TimelineException e) {
            assertEquals("not-null", e.getMessage());
        }

        try {
            Preconditions.checkNotNull(1);
        } catch (TimelineException e) {
            fail();
        }
    }

    @Test
    public void testCheckArgument() {

        try {
            Preconditions.checkArgument(false);
            fail();
        } catch (TimelineException e) {
        }

        try {
            Preconditions.checkArgument(false, "not false");
            fail();
        } catch (TimelineException e) {
            assertEquals("not false", e.getMessage());
        }

        try {
            Preconditions.checkArgument(false, "%s-%s", "not", "false");
            fail();
        } catch (TimelineException e) {
            assertEquals("not-false", e.getMessage());
        }

        try {
            Preconditions.checkArgument(true);
        } catch (TimelineException e) {
            fail();
        }
    }

    @Test
    public void testCheckNotEmptyString() {

        try {
            Preconditions.checkNotEmptyString("", "not empty");
            fail();
        } catch (TimelineException e) {
            assertEquals("not empty", e.getMessage());
        }

        try {
            Preconditions.checkNotEmptyString(" ", "not empty");
        } catch (TimelineException e) {
            fail();
        }

        try {
            Preconditions.checkNotEmptyString(null, "not empty");
        } catch (TimelineException e) {
            fail();
        }
    }

    @Test
    public void testCheckStringNotNullAndEmpty() {

        try {
            Preconditions.checkStringNotNullAndEmpty("", "not empty nor null");
            fail();
        } catch (TimelineException e) {
            assertEquals("not empty nor null", e.getMessage());
        }

        try {
            Preconditions.checkStringNotNullAndEmpty(" ", "not empty nor null");
        } catch (TimelineException e) {
            fail();
        }

        try {
            Preconditions.checkStringNotNullAndEmpty(null, "not empty nor null");
            fail();
        } catch (TimelineException e) {
            assertEquals("not empty nor null", e.getMessage());
        }
    }


}
