package eu.anonymized.crypto;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoUtils {

    private static final char[] PASSWORD = "supercalifragilistichespiralitos".toCharArray();
    private static final byte[] SALT = { (byte) 0xde, (byte) 0x33, (byte) 0x10, (byte) 0x12, (byte) 0xde, (byte) 0x33,
            (byte) 0x10, (byte) 0x12, };
//    private static final String aesProvider="AES/CBC/NoPadding"; 
    private static final String aesProvider="AES/ECB/PKCS5Padding"; 
//    static SecureRandom random = new SecureRandom();
//    static byte [] iv = new byte [16];
    private static final Logger LOG = LoggerFactory.getLogger(CryptoUtils.class);
    

    public static void main(String[] args) throws Exception {
    	String base32 = CryptoUtils.hash("ciao", "dopo");
    	System.out.println("Hashed : " + base32);
    	System.out.println(CryptoUtils.base32Decode(base32));
        char[] originalPassword = "aa".toCharArray();
        System.out.println("Original password: " + originalPassword);
        String encryptedPassword = encryptPBE(new String(originalPassword),new String(PASSWORD));
        System.out.println("Encrypted password: " + encryptedPassword);
        String decryptedPassword = decryptPBE(encryptedPassword,new String(PASSWORD));
        System.out.println("Decrypted password: " + decryptedPassword);
        
        String encryptedPasswordAES = encryptAES(new String(originalPassword));
        System.out.println("Encrypted aes password: " + encryptedPasswordAES);
        
        String decPasswordAES = decryptAES(encryptedPasswordAES);
        System.out.println("Decrypted aes password: " + decPasswordAES);

        
    }

    public static String encryptPBE(String property,String password) throws GeneralSecurityException, UnsupportedEncodingException {
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey key = keyFactory.generateSecret(new PBEKeySpec(password.toCharArray()));
        Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
        pbeCipher.init(Cipher.ENCRYPT_MODE, key, new PBEParameterSpec(SALT, 20));
        return base64Encode(pbeCipher.doFinal(property.getBytes("UTF-8")));
    }
    
    public static String encryptPBE(String property) throws GeneralSecurityException, UnsupportedEncodingException {
    	return encryptAES(property, new String(PASSWORD));
    }
    
    
    public static String encryptAES(String property,String password) throws GeneralSecurityException, UnsupportedEncodingException {
    	SecretKey key = new SecretKeySpec(password.getBytes(),"AES");
    	Cipher cipher = Cipher.getInstance(aesProvider);
//        byte[] iv = new byte[cipher.getBlockSize()];
//    	cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec( iv ));
    	
    	cipher.init(Cipher.ENCRYPT_MODE, key);
    	return base64Encode(cipher.doFinal(property.getBytes("UTF-8")));
    }
    
    public static String encryptAES(String property) throws GeneralSecurityException, UnsupportedEncodingException {
    	return encryptAES(property, new String(PASSWORD));
    }

    public static String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static String decryptPBE(String property,String password) throws GeneralSecurityException, IOException {
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey key = keyFactory.generateSecret(new PBEKeySpec(password.toCharArray()));
        Cipher pbeCipher = Cipher.getInstance("PBEWithMD5AndDES");
        pbeCipher.init(Cipher.DECRYPT_MODE, key, new PBEParameterSpec(SALT, 20));
        return new String(pbeCipher.doFinal(base64Decode(property)), "UTF-8");
    }
    
    public static String decryptAES(String property64,String password) throws GeneralSecurityException, IOException {
    	SecretKey key = new SecretKeySpec(password.getBytes(),"AES");
    	Cipher cipher = Cipher.getInstance(aesProvider);
//    	byte[] iv = new byte[cipher.getBlockSize()];
//    	cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec( iv ));
    	cipher.init(Cipher.DECRYPT_MODE, key);
    	return new String(cipher.doFinal(base64Decode(property64)), "UTF-8");
    }
    
    public static String decryptAES(String property64) throws GeneralSecurityException, IOException {
    	return decryptAES(property64,new String(PASSWORD));
    }
    
    public static String hash(String value,String secret) throws NoSuchAlgorithmException, IOException {
    	String hashed=new StringBuffer().append(secret).append(value).toString();
//    	MessageDigest digest= MessageDigest.getInstance("SHA-256");
//    	byte[] encodedhash = digest.digest(
//    			hashed.getBytes(StandardCharsets.UTF_8));
    	String sha256hex = DigestUtils.sha256Hex(hashed);
    	return sha256hex;
    	
    	//return base32Encode(sha256hex.getBytes());
    	
    	
    	
    }
    
    
    public static byte[] base64Decode(String property) throws IOException {
        return Base64.getDecoder().decode(property);
    }

    public static String base32Encode(byte[] property) throws IOException {
    	Base32 b = new Base32();
    	return b.encodeAsString(property);
        
    }
    public static String base32Decode(String property) throws IOException {
    	Base32 b = new Base32();
    	return new String(b.decode(property),"UTF-8");
    }
    
   

}