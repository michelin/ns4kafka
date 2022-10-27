package com.michelin.ns4kafka.utils;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.KeyLengthException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class EncryptionUtilsTest {
    /**
     * Validate encryption/decryption is not working when the KEK has wrong key size
     */
    @Test
    void validateEncryptAndDecryptAES256GCMWrongKeySize() throws IOException, JOSEException {
        String clearText = "myClearText";
        String keyEncryptionKey = "myKeyEncryptionKeyWrongSize";

        Assertions.assertThrows(KeyLengthException.class,
                () -> EncryptionUtils.encryptAES256GCM(clearText, keyEncryptionKey));
    }

    @Test
    void validateEncryptAndDecryptAES256GCM() throws IOException, JOSEException {
        String clearText = "myClearText";
        String keyEncryptionKey = "olDeandATEDiCenSiTurThrepASTrole";
        String encryptedText = EncryptionUtils.encryptAES256GCM(clearText, keyEncryptionKey);
        String clearTextDecrypted = EncryptionUtils.decryptAES256GCM(encryptedText, keyEncryptionKey);

        Assertions.assertEquals(clearText, clearTextDecrypted);
    }
}
