package com.alicloud.openservices.tablestore.timeline;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestScanParameterBuilder {
    @Test
    public void testInvalidWithOneParameter() {
        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(100);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.to(100);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.maxCount(100);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }
    }

    @Test
    public void testInvalidWithTwoParameter() {
        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(1);
            builder.to(20);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(100);
            builder.maxCount(2);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.to(100);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }
    }

    @Test
    public void testInvalidWithWrongForward() {
        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(100);
            builder.to(20);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }


        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanBackward();
            builder.from(100);
            builder.to(2000);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }
    }

    @Test
    public void testInvalidWithErrorFormat() {
        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(-10);
            builder.to(20);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(10);
            builder.to(-20);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }


        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(10);
            builder.to(20);
            builder.maxCount(-200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }


        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanBackward();
            builder.from(100);
            builder.to(-20);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }

        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanBackward();
            builder.from(-100);
            builder.to(20);
            builder.maxCount(200);
            builder.build();
            fail();
        } catch (TimelineException e) {
            assertEquals(TimelineExceptionType.TET_INVALID_USE, e.getType());
        }
    }

    @Test
    public void testSucceeded() {
        try {
            ScanParameterBuilder builder = ScanParameterBuilder.scanForward();
            builder.from(1);
            builder.to(20);
            builder.maxCount(100);
            builder.build();
            assertTrue(true);
        } catch (Exception e) {
            fail();
        }
    }
}
