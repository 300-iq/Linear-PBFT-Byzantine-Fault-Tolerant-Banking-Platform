package pbft.common.validation;

import pbft.common.crypto.Signer;
import pbft.proto.SignedMessage;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;

public final class SignedMessagePacker {
    private SignedMessagePacker() {}

    public static SignedMessage encode(String domain,
                                       String typeUrl,
                                       byte[] payload,
                                       String signerId,
                                       long view,
                                       long seq,
                                       PrivateKey sk) throws GeneralSecurityException {
        return pack(domain, typeUrl, payload, signerId, view, seq, sk);
    }

    public static SignedMessage encode(String domain,
                                       String typeUrl,
                                       com.google.protobuf.MessageLite msg,
                                       String signerId,
                                       long view,
                                       long seq,
                                       PrivateKey sk) throws GeneralSecurityException {
        return pack(domain, typeUrl, msg, signerId, view, seq, sk);
    }

    public static SignedMessage pack(String domain,
                                      String typeUrl,
                                      byte[] payload,
                                      String signerId,
                                      long view,
                                      long seq,
                                      PrivateKey sk) throws GeneralSecurityException {
        byte[] sig = Signer.sign(domain, typeUrl, payload, sk);
        return SignedMessage.newBuilder()
                .setTypeUrl(typeUrl)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .setSignerId(signerId)
                .setSignature(com.google.protobuf.ByteString.copyFrom(sig))
                .setView(view)
                .setSeq(seq)
                .build();
    }

    public static SignedMessage pack(String domain,
                                      String typeUrl,
                                      com.google.protobuf.MessageLite msg,
                                      String signerId,
                                      long view,
                                      long seq,
                                      PrivateKey sk) throws GeneralSecurityException {
        return pack(domain, typeUrl, msg.toByteArray(), signerId, view, seq, sk);
    }
}
