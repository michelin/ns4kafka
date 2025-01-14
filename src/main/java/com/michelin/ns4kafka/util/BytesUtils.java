/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.michelin.ns4kafka.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * BytesUtils is a utility class to convert bytes to human-readable values and vice-versa.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class BytesUtils {
    public static final String BYTE = "B";
    public static final String KIBIBYTE = "KiB";
    public static final String MEBIBYTE = "MiB";
    public static final String GIBIBYTE = "GiB";

    /**
     * Converts given bytes to either kibibyte, mebibite or gibibyte.
     *
     * @param bytes The bytes to convert
     * @return The converted value as human-readable value
     */
    public static String bytesToHumanReadable(long bytes) {
        double kibibyte = 1024;
        double mebibyte = kibibyte * 1024;
        double gibibyte = mebibyte * 1024;

        if (bytes >= kibibyte && bytes < mebibyte) {
            return BigDecimal.valueOf(bytes / kibibyte).setScale(3, RoundingMode.CEILING).doubleValue() + KIBIBYTE;
        }

        if (bytes >= mebibyte && bytes < gibibyte) {
            return BigDecimal.valueOf(bytes / mebibyte).setScale(3, RoundingMode.CEILING).doubleValue() + MEBIBYTE;
        }

        if (bytes >= gibibyte) {
            return BigDecimal.valueOf(bytes / gibibyte).setScale(3, RoundingMode.CEILING).doubleValue() + GIBIBYTE;
        }

        return bytes + BYTE;
    }

    /**
     * Converts given human-readable measure to bytes.
     *
     * @param quota The measure to convert
     * @return The converted value as bytes
     */
    public static long humanReadableToBytes(String quota) {
        long kibibyte = 1024;
        long mebibyte = kibibyte * 1024;
        long gibibyte = mebibyte * 1024;

        if (quota.endsWith(KIBIBYTE)) {
            return BigDecimal.valueOf(Double.parseDouble(quota.replace(KIBIBYTE, "")) * kibibyte)
                .setScale(0, RoundingMode.CEILING)
                .longValue();
        }

        if (quota.endsWith(MEBIBYTE)) {
            return BigDecimal.valueOf(Double.parseDouble(quota.replace(MEBIBYTE, "")) * mebibyte)
                .setScale(0, RoundingMode.CEILING)
                .longValue();
        }

        if (quota.endsWith(GIBIBYTE)) {
            return BigDecimal.valueOf(Double.parseDouble(quota.replace(GIBIBYTE, "")) * gibibyte)
                .setScale(0, RoundingMode.CEILING)
                .longValue();
        }

        return Long.parseLong(quota.replace(BYTE, ""));
    }
}
