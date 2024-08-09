package com.michelin.ns4kafka.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class BytesUtilsTest {
    @Test
    void shouldValidateBytesToHumanReadable() {
        assertEquals("0B", BytesUtils.bytesToHumanReadable(0L));
        assertEquals("27B", BytesUtils.bytesToHumanReadable(27L));
        assertEquals("999B", BytesUtils.bytesToHumanReadable(999L));
        assertEquals("1000B", BytesUtils.bytesToHumanReadable(1000L));

        assertEquals("1.0KiB", BytesUtils.bytesToHumanReadable(1024L));
        assertEquals("1.688KiB", BytesUtils.bytesToHumanReadable(1728L));
        assertEquals("108.0KiB", BytesUtils.bytesToHumanReadable(110592L));

        assertEquals("6.75MiB", BytesUtils.bytesToHumanReadable(7077888L));
        assertEquals("432.0MiB", BytesUtils.bytesToHumanReadable(452984832L));

        assertEquals("27.0GiB", BytesUtils.bytesToHumanReadable(28991029248L));
    }

    @Test
    void shouldValidateHumanReadableToBytes() {
        assertEquals(0L, BytesUtils.humanReadableToBytes("0B"));
        assertEquals(27L, BytesUtils.humanReadableToBytes("27B"));
        assertEquals(999L, BytesUtils.humanReadableToBytes("999B"));
        assertEquals(1000L, BytesUtils.humanReadableToBytes("1000B"));

        assertEquals(1024L, BytesUtils.humanReadableToBytes("1KiB"));
        assertEquals(1729L, BytesUtils.humanReadableToBytes("1.688KiB"));
        assertEquals(110592L, BytesUtils.humanReadableToBytes("108KiB"));

        assertEquals(7077888L, BytesUtils.humanReadableToBytes("6.75MiB"));
        assertEquals(452984832L, BytesUtils.humanReadableToBytes("432.0MiB"));

        assertEquals(28991029248L, BytesUtils.humanReadableToBytes("27GiB"));
    }
}
