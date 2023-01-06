package com.michelin.ns4kafka.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class BytesUtilsTest {

    /**
     * Validate bytes conversion to human-readable string
     */
    @Test
    void validateBytesToHumanReadable() {
        Assertions.assertEquals("0B", BytesUtils.bytesToHumanReadable(0L));
        Assertions.assertEquals("27B", BytesUtils.bytesToHumanReadable(27L));
        Assertions.assertEquals("999B", BytesUtils.bytesToHumanReadable(999L));
        Assertions.assertEquals("1000B", BytesUtils.bytesToHumanReadable(1000L));

        Assertions.assertEquals("1.0KiB", BytesUtils.bytesToHumanReadable(1024L));
        Assertions.assertEquals("1.688KiB", BytesUtils.bytesToHumanReadable(1728L));
        Assertions.assertEquals("108.0KiB", BytesUtils.bytesToHumanReadable(110592L));

        Assertions.assertEquals("6.75MiB", BytesUtils.bytesToHumanReadable(7077888L));
        Assertions.assertEquals("432.0MiB", BytesUtils.bytesToHumanReadable(452984832L));

        Assertions.assertEquals("27.0GiB", BytesUtils.bytesToHumanReadable(28991029248L));
    }

    /**
     * Validate human-readable string to bytes conversion
     */
    @Test
    void validateHumanReadableToBytes() {
        Assertions.assertEquals(0L, BytesUtils.humanReadableToBytes("0B"));
        Assertions.assertEquals(27L, BytesUtils.humanReadableToBytes("27B"));
        Assertions.assertEquals(999L, BytesUtils.humanReadableToBytes("999B"));
        Assertions.assertEquals(1000L, BytesUtils.humanReadableToBytes("1000B"));

        Assertions.assertEquals(1024L, BytesUtils.humanReadableToBytes("1KiB"));
        Assertions.assertEquals(1729L, BytesUtils.humanReadableToBytes("1.688KiB"));
        Assertions.assertEquals(110592L, BytesUtils.humanReadableToBytes("108KiB"));

        Assertions.assertEquals(7077888L, BytesUtils.humanReadableToBytes("6.75MiB"));
        Assertions.assertEquals(452984832L, BytesUtils.humanReadableToBytes("432.0MiB"));

        Assertions.assertEquals(28991029248L, BytesUtils.humanReadableToBytes("27GiB"));
    }
}
