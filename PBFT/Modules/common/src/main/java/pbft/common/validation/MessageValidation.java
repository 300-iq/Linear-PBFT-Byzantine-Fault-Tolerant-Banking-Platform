package pbft.common.validation;

import pbft.common.config.KeyStore;
import pbft.common.crypto.Signer;
import pbft.proto.SignedMessage;

import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.util.Optional;


public final class MessageValidation {
    public enum Code {
        OK, UNKNOWN_SIGNER, BAD_SIGNATURE, BAD_VIEW_SEQ, BAD_TYPEURL, EMPTY_PAYLOAD
    }
    public record Result(Code code, String reason) {
        public static Result ok() { return new Result(Code.OK, ""); }
    }

    private MessageValidation() {}

    public static Result validateSigned(SignedMessage signedMsg, String domain, KeyStore reg, boolean signerIsReplica) {
        if (signedMsg.getPayload().isEmpty()) return new Result(Code.EMPTY_PAYLOAD, "payload empty");
        if (signedMsg.getView() < 0 || signedMsg.getSeq() < 0) return new Result(Code.BAD_VIEW_SEQ, "view/seq negative");
        if (signedMsg.getTypeUrl().isBlank()) return new Result(Code.BAD_TYPEURL, "type_url blank");

        Optional<PublicKey> pkOpt = signerIsReplica ? reg.replicaKey(signedMsg.getSignerId()) : reg.clientKey(signedMsg.getSignerId());
        if (pkOpt.isEmpty()) return new Result(Code.UNKNOWN_SIGNER, "no pubkey for " + signedMsg.getSignerId());

        try {
            boolean ok = Signer.verify(domain, signedMsg.getTypeUrl(), signedMsg.getPayload().toByteArray(), signedMsg.getSignature().toByteArray(), pkOpt.get());
            return ok ? Result.ok() : new Result(Code.BAD_SIGNATURE, "signature mismatch");
        } catch (GeneralSecurityException e) {
            return new Result(Code.BAD_SIGNATURE, "crypto error: " + e.getMessage());
        }
    }
}
