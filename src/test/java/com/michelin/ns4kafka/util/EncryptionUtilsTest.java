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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/**
 * Encryption utils test.
 */
class EncryptionUtilsTest {
    @Test
    void shouldValidateEncryptAndDecryptAes256GcmNullText() {
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        String stillNullText = EncryptionUtils.encryptAes256Gcm(null, keyEncryptionKey);
        assertNull(stillNullText);
    }

    @Test
    void shouldValidateEncryptAndDecryptAes256GcmBlankText() {
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        String stillBlankText = EncryptionUtils.encryptAes256Gcm("", keyEncryptionKey);
        assertEquals("", stillBlankText);
    }

    @Test
    void shouldValidateEncryptAndDecryptAes256GcmWrongKeySize() {
        String clearText = "myClearText";
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";
        String myClearText = EncryptionUtils.encryptAes256Gcm(clearText, keyEncryptionKey);

        assertEquals(clearText, myClearText);
    }

    @Test
    void shouldValidateEncryptAndDecryptAes256Gcm() {
        String clearText = "myClearText";
        String keyEncryptionKey = "olDeandATEDiCenSiTurThrepASTrole";
        String encryptedText = EncryptionUtils.encryptAes256Gcm(clearText, keyEncryptionKey);
        String clearTextDecrypted = EncryptionUtils.decryptAes256Gcm(encryptedText, keyEncryptionKey);

        assertEquals(clearText, clearTextDecrypted);
    }

    @Test
    void shouldValidateEncryptAndDecryptAes256BlankText() {
        final String encryptionKey = "myKeyEncryption";
        final String encryptionSalt = "mySaltEncryption";

        final String stillBlankText = EncryptionUtils.encryptAesWithPrefix("", encryptionKey, encryptionSalt);
        assertEquals("", stillBlankText);
    }

    @Test
    void shouldValidateEncryptAndDecryptAes256NullText() {
        final String encryptionKey = "myKeyEncryption";
        final String encryptionSalt = "p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS";

        final String stillBlankText = EncryptionUtils.encryptAesWithPrefix(null, encryptionKey, encryptionSalt);
        assertNull(stillBlankText);
    }

    @Test
    void shouldValidateEncryptAndDecryptAes256() {
        String clearText = "myClearText";
        String encryptionKey = "myKeyEncryption";
        String encryptionSalt = "p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS";
        String encryptedText = EncryptionUtils.encryptAesWithPrefix(clearText, encryptionKey, encryptionSalt);
        String clearTextDecrypted = EncryptionUtils.decryptAesWithPrefix(encryptedText, encryptionKey, encryptionSalt);

        assertEquals(clearText, clearTextDecrypted);
    }

    @Test
    void shouldValidateEncryptNeverSameValue() {
        String clearText = "myClearText";
        String encryptionKey = "myKey";
        String encryptionSalt = "toto";
        String encryptedText = EncryptionUtils.encryptAesWithPrefix(clearText, encryptionKey, encryptionSalt);
        String encryptedText2 = EncryptionUtils.encryptAesWithPrefix(clearText, encryptionKey, encryptionSalt);
        String clearTextDecrypted = EncryptionUtils.decryptAesWithPrefix(encryptedText, encryptionKey, encryptionSalt);
        String clearTextDecrypted2 =
            EncryptionUtils.decryptAesWithPrefix(encryptedText2, encryptionKey, encryptionSalt);

        assertEquals(clearText, clearTextDecrypted);
        assertNotEquals(encryptedText2, encryptedText);
        assertEquals(clearText, clearTextDecrypted2);
    }
}
