package pbft.common.validation;

import pbft.common.config.KeyStore;
import pbft.common.crypto.RequestType;
import pbft.proto.Commit;
import pbft.proto.CommitProof;
import pbft.proto.Prepare;
import pbft.proto.PrepareProof;
import pbft.proto.SignedMessage;

import java.util.HashSet;
import java.util.Set;


public final class ProofValidator {
    private ProofValidator() {}

    public record Result(boolean ok, String reason) {
        public static Result success() { return new Result(true, ""); }
    }

    public static Result validatePrepareProof(PrepareProof proof, KeyStore reg, int n, int f) {
        int threshold = n - f; // 2f+1 when n=3f+1
        if (proof.getSignedPreparesCount() < threshold) {
            return new Result(false, "insufficient prepares: " + proof.getSignedPreparesCount() + " < " + threshold);
        }
        Set<String> signers = new HashSet<>();
        for (SignedMessage env : proof.getSignedPreparesList()) {
            if (!PbftMsgTypes.PREPARE.equals(env.getTypeUrl())) {
                return new Result(false, "bad type_url in proof: " + env.getTypeUrl());
            }
            var r = MessageValidation.validateSigned(env, RequestType.PREPARE, reg, true);
            if (r.code() != MessageValidation.Code.OK) {
                return new Result(false, "bad inner signature: " + r);
            }
            try {
                Prepare p = Prepare.parseFrom(env.getPayload());
                if (p.getView() != proof.getView() || p.getSeq() != proof.getSeq()) {
                    return new Result(false, "inner view/seq mismatch");
                }
                if (!p.getRequestDigest().equals(proof.getRequestDigest())) {
                    return new Result(false, "digest mismatch in proof");
                }
                if (!env.getSignerId().equals(p.getReplicaId())) {
                    return new Result(false, "replicaId/signature mismatch");
                }
            } catch (Exception e) {
                return new Result(false, "bad inner payload: " + e.getMessage());
            }
            if (!signers.add(env.getSignerId())) {
                return new Result(false, "duplicate signer: " + env.getSignerId());
            }
        }
        if (signers.size() < threshold) {
            return new Result(false, "distinct signers < threshold");
        }
        return Result.success();
    }

    public static Result validateCommitProof(CommitProof proof, KeyStore reg, int n, int f) {
        int threshold = n - f;
        if (proof.getSignedCommitsCount() < threshold) {
            return new Result(false, "insufficient commits: " + proof.getSignedCommitsCount() + " < " + threshold);
        }
        Set<String> signers = new HashSet<>();
        for (SignedMessage env : proof.getSignedCommitsList()) {
            if (!PbftMsgTypes.COMMIT.equals(env.getTypeUrl())) {
                return new Result(false, "bad type_url in proof: " + env.getTypeUrl());
            }
            var r = MessageValidation.validateSigned(env, RequestType.COMMIT, reg, true);
            if (r.code() != MessageValidation.Code.OK) {
                return new Result(false, "bad inner signature: " + r);
            }
            try {
                Commit c = Commit.parseFrom(env.getPayload());
                if (c.getView() != proof.getView() || c.getSeq() != proof.getSeq()) {
                    return new Result(false, "inner view/seq mismatch");
                }
                if (!c.getRequestDigest().equals(proof.getRequestDigest())) {
                    return new Result(false, "digest mismatch in proof");
                }
                if (!env.getSignerId().equals(c.getReplicaId())) {
                    return new Result(false, "replicaId/signature mismatch");
                }
            } catch (Exception e) {
                return new Result(false, "bad inner payload: " + e.getMessage());
            }
            if (!signers.add(env.getSignerId())) {
                return new Result(false, "duplicate signer: " + env.getSignerId());
            }
        }
        if (signers.size() < threshold) {
            return new Result(false, "distinct signers < threshold");
        }
        return Result.success();
    }
}
