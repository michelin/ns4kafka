package com.michelin.ns4kafka.utils;

import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import com.nimbusds.jose.util.Base64URL;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

@Slf4j
public class EncryptionUtils {
    /**
     * Constructor
     */
    private EncryptionUtils() {
    }

    /**
     * Encrypt given text with the given key to AES256 GCM then encode it to Base64
     *
     * @param clearText The text to encrypt
     * @param key       The key encryption key (KEK)
     * @return The encrypted password
     */
    public static String encryptAES256GCM(String clearText, String key) {
        try {
            if (!StringUtils.isNotBlank(clearText)) {
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
     *
     * @param encryptedText The text to decrypt
     * @param key           The key encryption key (KEK)
     * @return The decrypted text
     */
    public static String decryptAES256GCM(String encryptedText, String key) {
        try {
            if (!StringUtils.isNotBlank(encryptedText)) {
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

    /**
     * Encrypt clear text with the given key and salt to AES256 encrypted text.
     *
     * @param clearText The text to encrypt.
     * @param key       The encryption key.
     * @param salt      The encryption salt.
     * @return The encrypted password.
     */
    public static String encryptAES256(final String clearText, final String key, final String salt) {
        try {
            byte[] iv = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            final var ivSpec = new IvParameterSpec(iv);
            final var factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            final var spec = new PBEKeySpec(key.toCharArray(), salt.getBytes(), 65536, 256);
            final var secret = factory.generateSecret(spec);
            final var secretKey = new SecretKeySpec(secret.getEncoded(), "AES");
            final var cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivSpec);
            return Base64.getEncoder().encodeToString(cipher.doFinal(clearText.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
