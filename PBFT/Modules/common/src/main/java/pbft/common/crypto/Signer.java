package pbft.common.crypto;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;

public final class Signer {
    private Signer() {}

    public static byte[] sign(String domain, String typeUrl, byte[] payload, PrivateKey sk)
            throws GeneralSecurityException {
        byte[] d = domain.getBytes(StandardCharsets.UTF_8);
        byte[] t = typeUrl.getBytes(StandardCharsets.UTF_8);
        byte[] input = join(d, t, payload);
        Signature s = Signature.getInstance("Ed25519");
        s.initSign(sk);
        s.update(input);
        return s.sign();
    }

    public static boolean verify(String domain, String typeUrl, byte[] payload, byte[] signature, PublicKey pk)
            throws GeneralSecurityException {
        byte[] d = domain.getBytes(StandardCharsets.UTF_8);
        byte[] t = typeUrl.getBytes(StandardCharsets.UTF_8);
        byte[] input = join(d, t, payload);
        Signature s = Signature.getInstance("Ed25519");
        s.initVerify(pk);
        s.update(input);
        return s.verify(signature);
    }

    private static byte[] join(byte[] d, byte[] t, byte[] p) {
        ByteBuffer buf = ByteBuffer.allocate(d.length + 1 + t.length + 1 + p.length);
        buf.put(d).put((byte)0).put(t).put((byte)0).put(p);
        return buf.array();
    }
}
