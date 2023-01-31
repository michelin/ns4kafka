package com.michelin.ns4kafka.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EncryptionUtilsTest {
    /**
     * Validate encryption/decryption when given text is null
     */
    @Test
    void validateEncryptAndDecryptAES256GCMNullText() {
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        String stillNullText = EncryptionUtils.encryptAES256GCM(null, keyEncryptionKey);
        Assertions.assertNull(stillNullText);
    }

    /**
     * Validate encryption/decryption when given text is blank
     */
    @Test
    void validateEncryptAndDecryptAES256GCMBlankText() {
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        String stillBlankText = EncryptionUtils.encryptAES256GCM("", keyEncryptionKey);
        Assertions.assertEquals("", stillBlankText);
    }

    /**
     * Validate encryption/decryption is not working when the KEK has wrong key size
     */
    @Test
    void validateEncryptAndDecryptAES256GCMWrongKeySize() {
        String clearText = "myClearText";
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";
        String myClearText = EncryptionUtils.encryptAES256GCM(clearText, keyEncryptionKey);

        Assertions.assertEquals(clearText, myClearText);
    }

    @Test
    void validateEncryptAndDecryptAES256GCM() {
        String clearText = "myClearText";
        String keyEncryptionKey = "olDeandATEDiCenSiTurThrepASTrole";
        String encryptedText = EncryptionUtils.encryptAES256GCM(clearText, keyEncryptionKey);
        String clearTextDecrypted = EncryptionUtils.decryptAES256GCM(encryptedText, keyEncryptionKey);

        Assertions.assertEquals(clearText, clearTextDecrypted);
    }

    /**
     * Validate encryption when given text is blank
     */
    @Test
    void validateEncryptAndDecryptAES256lankText() {
        final String encryptionKey = "myKeyEncryption";
        final String encryptionSalt = "mySaltEncryption";

        final String stillBlankText = EncryptionUtils.encryptAESWithPrefix("", encryptionKey, encryptionSalt);
        Assertions.assertEquals("", stillBlankText);
    }

    /**
     * Validate encryption when given text is blank
     */
    @Test
    void validateEncryptAndDecryptAES256NullText() {
        final String encryptionKey = "myKeyEncryption";
        final String encryptionSalt = "p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS";

        final String stillBlankText = EncryptionUtils.encryptAESWithPrefix(null, encryptionKey, encryptionSalt);
        Assertions.assertEquals(null, stillBlankText);
    }

    @Test
    void validateEncryptAndDecryptAES256() {
        String clearText = "myClearText";
        String encryptionKey = "myKeyEncryption";
        String encryptionSalt = "p8t42EhY9z2eSUdpGeq7HX7RboMrsJAhUnu3EEJJVS";
        String encryptedText = EncryptionUtils.encryptAESWithPrefix(clearText, encryptionKey, encryptionSalt);
        String clearTextDecrypted = EncryptionUtils.decryptAESWithPrefix(encryptedText, encryptionKey, encryptionSalt);

        Assertions.assertEquals(clearText, clearTextDecrypted);
    }

    @Test
    void validateEncryptNEverSameValue() {
        String clearText = "myClearText";
        String encryptionKey = "myKey";
        String encryptionSalt = "toto";
        String encryptedText = EncryptionUtils.encryptAESWithPrefix(clearText, encryptionKey, encryptionSalt);
        String encryptedText2 = EncryptionUtils.encryptAESWithPrefix(clearText, encryptionKey, encryptionSalt);
        String clearTextDecrypted = EncryptionUtils.decryptAESWithPrefix(encryptedText, encryptionKey, encryptionSalt);
        String clearTextDecrypted2 = EncryptionUtils.decryptAESWithPrefix(encryptedText2, encryptionKey, encryptionSalt);

        Assertions.assertEquals(clearText, clearTextDecrypted);
        Assertions.assertNotEquals(encryptedText2, encryptedText);
        Assertions.assertEquals(clearText, clearTextDecrypted2);
    }
}
