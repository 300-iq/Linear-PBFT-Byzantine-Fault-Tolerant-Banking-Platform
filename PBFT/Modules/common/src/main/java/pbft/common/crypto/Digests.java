package pbft.common.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Digests {
    private Digests() {}

    public static byte[] sha256(byte[] input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(input);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
