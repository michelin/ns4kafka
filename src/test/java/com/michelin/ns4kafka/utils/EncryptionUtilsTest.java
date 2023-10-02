package com.michelin.ns4kafka.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Encryption utils test.
 */
class EncryptionUtilsTest {
    @Test
    void validateEncryptAndDecryptAes256GcmNullText() {
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        String stillNullText = EncryptionUtils.encryptAes256Gcm(null, keyEncryptionKey);
        Assertions.assertNull(stillNullText);
    }

    @Test
    void validateEncryptAndDecryptAes256GcmBlankText() {
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        String stillBlankText = EncryptionUtils.encryptAes256Gcm("", keyEncryptionKey);
        assertEquals("", stillBlankText);
    }

    @Test
    void validateEncryptAndDecryptAes256GcmWrongKeySize() {
        String clearText = "myClearText";
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";
        String myClearText = EncryptionUtils.encryptAes256Gcm(clearText, keyEncryptionKey);

        assertEquals(clearText, myClearText);
    }

    @Test
    void validateEncryptAndDecryptAes256Gcm() {
        String clearText = "myClearText";
        String keyEncryptionKey = "olDeandATEDiCenSiTurThrepASTrole";
        String encryptedText = EncryptionUtils.encryptAes256Gcm(clearText, keyEncryptionKey);
        String clearTextDecrypted = EncryptionUtils.decryptAes256Gcm(encryptedText, keyEncryptionKey);

        assertEquals(clearText, clearTextDecrypted);
    }

    @Test
    void validateEncryptAndDecryptAes256BlankText() {
        final String encryptionKey = "myKeyEncryption";
        final String encryptionSalt = "mySaltEncryption";

        final String stillBlankText = EncryptionUtils.encryptAesWithPrefix("", encryptionKey, encryptionSalt);
        assertEquals("", stillBlankText);
    }

    @Test
    void validateEncryptAndDecryptAes256NullText() {
        final String encryptionKey = "myKeyEncryption";
        final String encryptionSalt = "p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS";

        final String stillBlankText = EncryptionUtils.encryptAesWithPrefix(null, encryptionKey, encryptionSalt);
        Assertions.assertNull(stillBlankText);
    }

    @Test
    void validateEncryptAndDecryptAes256() {
        String clearText = "myClearText";
        String encryptionKey = "myKeyEncryption";
        String encryptionSalt = "p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS";
        String encryptedText = EncryptionUtils.encryptAesWithPrefix(clearText, encryptionKey, encryptionSalt);
        String clearTextDecrypted = EncryptionUtils.decryptAesWithPrefix(encryptedText, encryptionKey, encryptionSalt);

        assertEquals(clearText, clearTextDecrypted);
    }

    @Test
    void validateEncryptNeverSameValue() {
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
