package com.michelin.ns4kafka.utils;

import com.nimbusds.jose.EncryptionMethod;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWEAlgorithm;
import com.nimbusds.jose.JWECryptoParts;
import com.nimbusds.jose.JWEHeader;
import com.nimbusds.jose.crypto.AESDecrypter;
import com.nimbusds.jose.crypto.AESEncrypter;
import com.nimbusds.jose.util.Base64URL;
import io.micronaut.core.util.StringUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Encryption utils.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EncryptionUtils {
    /**
     * The AES encryption algorithm.
     */
    private static final String ENCRYPT_ALGO = "AES/GCM/NoPadding";

    /**
     * The authentication tag length.
     */
    private static final int TAG_LENGTH_BIT = 128;

    /**
     * The Initial Value length.
     */
    private static final int IV_LENGTH_BYTE = 12;

    /**
     * The NS4KAFKA prefix.
     */
    private static final String NS4KAFKA_PREFIX = "NS4K";

    /**
     * Encrypt given text with the given key to AES256 GCM then encode it to Base64.
     *
     * @param clearText The text to encrypt
     * @param key       The key encryption key (KEK)
     * @return The encrypted password
     */
    public static String encryptAes256Gcm(String clearText, String key) {
        try {
            if (!StringUtils.hasText(clearText)) {
                return clearText;
            }

            AESEncrypter encrypter = new AESEncrypter(key.getBytes(StandardCharsets.UTF_8));
            JWECryptoParts encryptedData =
                encrypter.encrypt(new JWEHeader(JWEAlgorithm.A256KW, EncryptionMethod.A256GCM),
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
     * Decrypt given text with the given key from AES256 GCM.
     *
     * @param encryptedText The text to decrypt
     * @param key           The key encryption key (KEK)
     * @return The decrypted text
     */
    public static String decryptAes256Gcm(String encryptedText, String key) {
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

    /**
     * Encrypt clear text with the given key and salt to AES256 encrypted text.
     *
     * @param clearText The text to encrypt.
     * @param key       The encryption key.
     * @param salt      The encryption salt.
     * @return The encrypted password.
     */
    public static String encryptAesWithPrefix(final String clearText, final String key, final String salt) {
        if (!StringUtils.hasText(clearText)) {
            return clearText;
        }

        try {
            final SecretKey secret = getAesSecretKey(key, salt);
            final byte[] iv = getRandomIv();
            final var cipher = Cipher.getInstance(ENCRYPT_ALGO);
            cipher.init(Cipher.ENCRYPT_MODE, secret, new GCMParameterSpec(TAG_LENGTH_BIT, iv));
            final byte[] cipherText = cipher.doFinal(clearText.getBytes(StandardCharsets.UTF_8));
            final byte[] prefix = NS4KAFKA_PREFIX.getBytes(StandardCharsets.UTF_8);
            final byte[] cipherTextWithIv = ByteBuffer.allocate(prefix.length + iv.length + cipherText.length)
                .put(prefix)
                .put(iv)
                .put(cipherText)
                .array();
            return Base64.getEncoder().encodeToString(cipherTextWithIv);
        } catch (Exception e) {
            log.error("An error occurred during Connect cluster AES256 string encryption", e);
        }

        return clearText;
    }

    /**
     * Decrypt text with the given key and salt from AES256 encrypted text.
     *
     * @param encryptedText The text to decrypt.
     * @param key           The encryption key.
     * @param salt          The encryption salt.
     * @return The encrypted password.
     */
    public static String decryptAesWithPrefix(final String encryptedText, final String key, final String salt) {
        if (!StringUtils.hasText(encryptedText)) {
            return encryptedText;
        }

        try {
            // Get IV and cipherText from encrypted text.
            final byte[] prefix = NS4KAFKA_PREFIX.getBytes(StandardCharsets.UTF_8);
            final var byteBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(encryptedText));
            final byte[] iv = new byte[IV_LENGTH_BYTE];
            byteBuffer.position(prefix.length);
            byteBuffer.get(iv);
            final byte[] cipherText = new byte[byteBuffer.remaining()];
            byteBuffer.get(cipherText);

            // decrypt the cipher text.
            final SecretKey secret = getAesSecretKey(key, salt);
            final var cipher = Cipher.getInstance(ENCRYPT_ALGO);
            cipher.init(Cipher.DECRYPT_MODE, secret, new GCMParameterSpec(TAG_LENGTH_BIT, iv));
            return new String(cipher.doFinal(cipherText), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("An error occurred during Connect cluster AES256 string decryption", e);
        }

        return encryptedText;
    }


    /**
     * Gets the secret key derived AES 256 bits key.
     *
     * @param key  The encryption key
     * @param salt The encryption salt
     * @return The encryption secret key.
     * @throws NoSuchAlgorithmException No such algorithm exception.
     * @throws InvalidKeySpecException  Invalid key spec exception.
     */
    private static SecretKey getAesSecretKey(final String key, final String salt)
        throws NoSuchAlgorithmException, InvalidKeySpecException {
        var factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        var spec = new PBEKeySpec(key.toCharArray(), salt.getBytes(StandardCharsets.UTF_8), 65536, 256);
        return new SecretKeySpec(factory.generateSecret(spec).getEncoded(), "AES");
    }

    /**
     * Get a random Initial Value byte array.
     *
     * @return The random IV.
     */
    private static byte[] getRandomIv() {
        final byte[] iv = new byte[IV_LENGTH_BYTE];
        new SecureRandom().nextBytes(iv);
        return iv;
    }
}
