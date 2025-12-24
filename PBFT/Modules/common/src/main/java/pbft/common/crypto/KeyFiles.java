package pbft.common.crypto;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public final class KeyFiles {
    private KeyFiles() {}

    public static KeyPair generateKeyPair() throws GeneralSecurityException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("Ed25519");
        return kpg.generateKeyPair();
    }

    public static void writeKeyPair(Path privatePem, Path publicPem, KeyPair kp) throws IOException {
        byte[] pkcs8 = kp.getPrivate().getEncoded();
        byte[] x509  = kp.getPublic().getEncoded();
        writePem(privatePem, "PRIVATE KEY", pkcs8);
        writePem(publicPem,  "PUBLIC KEY",  x509);
    }

    public static PrivateKey loadPrivateKeyPem(Path privatePem) throws GeneralSecurityException, IOException {
        byte[] der = readPem(privatePem, "PRIVATE KEY");
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
        return KeyFactory.getInstance("Ed25519").generatePrivate(spec);
    }

    public static PublicKey loadPublicKeyPem(Path publicPem) throws GeneralSecurityException, IOException {
        byte[] der = readPem(publicPem, "PUBLIC KEY");
        X509EncodedKeySpec spec = new X509EncodedKeySpec(der);
        return KeyFactory.getInstance("Ed25519").generatePublic(spec);
    }

    private static void writePem(Path path, String type, byte[] der) throws IOException {
        String base64 = Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(der);
        String pem = "-----BEGIN " + type + "-----\n" + base64 + "\n-----END " + type + "-----\n";
        Files.createDirectories(path.getParent());
        Files.writeString(path, pem, StandardCharsets.US_ASCII, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static byte[] readPem(Path path, String type) throws IOException {
        String all = Files.readString(path, StandardCharsets.US_ASCII);
        String begin = "-----BEGIN " + type + "-----";
        String end   = "-----END " + type + "-----";
        int i = all.indexOf(begin), j = all.indexOf(end);
        if (i < 0 || j < 0) throw new IOException("Invalid PEM: " + path);
        String b64 = all.substring(i + begin.length(), j).replaceAll("\\s", "");
        return Base64.getDecoder().decode(b64);
    }
}
