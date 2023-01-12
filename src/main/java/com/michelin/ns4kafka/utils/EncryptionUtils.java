package com.michelin.ns4kafka.utils;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import com.nimbusds.jose.util.Base64URL;
import io.micronaut.core.util.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@Slf4j
public class EncryptionUtils {
    private EncryptionUtils() { }

    /**
     * Encrypt given text with the given key to AES256 GCM then encode it to Base64
     * @param clearText The text to encrypt
     * @param key The key encryption key (KEK)
     * @return The encrypted password
     */
    public static String encryptAES256GCM(String clearText, String key) {
        try {
            if (!StringUtils.hasText(clearText)) {
                return clearText;
            }

            AESEncrypter encrypter = new AESEncrypter(key.getBytes(StandardCharsets.UTF_8));
            JWECryptoParts encryptedData = encrypter.encrypt(new JWEHeader(JWEAlgorithm.A256KW, EncryptionMethod.A256GCM),
                    clearText.getBytes(StandardCharsets.UTF_8));

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(encryptedData.getEncryptedKey().decode());
            outputStream.write(encryptedData.getInitializationVector().decode());
            outputStream.write(encryptedData.getAuthenticationTag().decode());
            outputStream.write(encryptedData.getCipherText().decode());

            return Base64URL.encode(outputStream.toByteArray()).toString();
        } catch (JOSEException | IOException e) {
            log.error("An error occurred during Connect cluster password encryption", e);
        }

        return clearText;
    }

    /**
     * Decrypt given text with the given key from AES256 GCM
     * @param encryptedText The text to decrypt
     * @param key The key encryption key (KEK)
     * @return The decrypted text
     */
    public static String decryptAES256GCM(String encryptedText, String key) {
        try {
            if (!StringUtils.hasText(encryptedText)) {
                return encryptedText;
            }

            AESDecrypter decrypter = new AESDecrypter(key.getBytes(StandardCharsets.UTF_8));
            byte[] encryptedData = Base64URL.from(encryptedText).decode();

            Base64URL encryptedKey = Base64URL.encode(Arrays.copyOfRange(encryptedData, 0, 40));
            Base64URL iv = Base64URL.encode(Arrays.copyOfRange(encryptedData, 40, 52));
            Base64URL auth = Base64URL.encode(Arrays.copyOfRange(encryptedData, 52, 68));
            Base64URL text = Base64URL.encode(Arrays.copyOfRange(encryptedData, 68, encryptedData.length));

            byte[] clearTextAsBytes = decrypter.decrypt(new JWEHeader(JWEAlgorithm.A256KW, EncryptionMethod.A256GCM),
                    encryptedKey, iv, text, auth);

            return new String(clearTextAsBytes);
        } catch (JOSEException e) {
            log.error("An error occurred during Connect cluster password decryption", e);
        }

        return encryptedText;
    }
}
