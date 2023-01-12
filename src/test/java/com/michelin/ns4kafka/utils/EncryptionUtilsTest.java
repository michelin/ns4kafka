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
}
