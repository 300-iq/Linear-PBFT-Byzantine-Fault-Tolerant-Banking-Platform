package pbft.common.validation;

import pbft.common.config.KeyStore;
import pbft.common.crypto.RequestType;
import pbft.proto.*;


public final class MessageDecoder {
    private MessageDecoder() {}

    public record TypedResult<T>(MessageValidation.Result validation,
                                 T message,
                                 String signerId,
                                 long envView,
                                 long envSeq,
                                 boolean viewMatches,
                                 boolean seqMatches) {
        public boolean ok() { return validation.code() == MessageValidation.Code.OK && message != null; }
    }

    public static TypedResult<PrePrepare> decodePrePrepare(SignedMessage signedMsg, KeyStore reg) {
        return unpackPrePrepare(signedMsg, reg);
    }

    public static TypedResult<ClientReply> decodeClientReply(SignedMessage signedMsg, KeyStore reg) {
        return unpackClientReply(signedMsg, reg);
    }

    public static TypedResult<Prepare> decodePrepare(SignedMessage signedMsg, KeyStore reg) {
        return unpackPrepare(signedMsg, reg);
    }

    public static TypedResult<Commit> decodeCommit(SignedMessage signedMsg, KeyStore reg) {
        return unpackCommit(signedMsg, reg);
    }

    public static TypedResult<PrepareProof> decodePrepareProof(SignedMessage signedMsg, KeyStore reg) {
        return unpackPrepareProof(signedMsg, reg);
    }

    public static TypedResult<CommitProof> decodeCommitProof(SignedMessage signedMsg, KeyStore reg) {
        return unpackCommitProof(signedMsg, reg);
    }

    public static TypedResult<ViewChange> decodeViewChange(SignedMessage signedMsg, KeyStore reg) {
        return unpackViewChange(signedMsg, reg);
    }

    public static TypedResult<NewView> decodeNewView(SignedMessage signedMsg, KeyStore reg) {
        return unpackNewView(signedMsg, reg);
    }

    public static TypedResult<Checkpoint> decodeCheckpoint(SignedMessage signedMsg, KeyStore reg) {
        return unpackCheckpoint(signedMsg, reg);
    }

    public static TypedResult<ClientRequest> decodeClientRequest(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.CLIENT_REQUEST);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), true, true);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.CLIENT_REQUEST, reg, false);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), true, true);
        try {
            ClientRequest msg = ClientRequest.parseFrom(signedMsg.getPayload());
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), true, true);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), true, true);
        }
    }

    public static TypedResult<PrePrepare> unpackPrePrepare(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.PRE_PREPARE);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.PRE_PREPARE, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            PrePrepare msg = PrePrepare.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getView();
            boolean sm = signedMsg.getSeq() == 0 || signedMsg.getSeq() == msg.getSeq();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, sm);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<ClientReply> unpackClientReply(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.CLIENT_REPLY);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.CLIENT_REPLY, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            ClientReply msg = ClientReply.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getView();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, true);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<Prepare> unpackPrepare(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.PREPARE);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.PREPARE, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            Prepare msg = Prepare.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getView();
            boolean sm = signedMsg.getSeq() == 0 || signedMsg.getSeq() == msg.getSeq();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, sm);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<Commit> unpackCommit(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.COMMIT);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.COMMIT, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            Commit msg = Commit.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getView();
            boolean sm = signedMsg.getSeq() == 0 || signedMsg.getSeq() == msg.getSeq();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, sm);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<PrepareProof> unpackPrepareProof(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.PREPARE_PROOF);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.PREPARE_PROOF, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            PrepareProof msg = PrepareProof.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getView();
            boolean sm = signedMsg.getSeq() == 0 || signedMsg.getSeq() == msg.getSeq();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, sm);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<CommitProof> unpackCommitProof(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.COMMIT_PROOF);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.COMMIT_PROOF, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            CommitProof msg = CommitProof.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getView();
            boolean sm = signedMsg.getSeq() == 0 || signedMsg.getSeq() == msg.getSeq();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, sm);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<ViewChange> unpackViewChange(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.VIEW_CHANGE);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.VIEW_CHANGE, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            ViewChange msg = ViewChange.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getNewView();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, true);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<NewView> unpackNewView(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.NEW_VIEW);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.NEW_VIEW, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            NewView msg = NewView.parseFrom(signedMsg.getPayload());
            boolean vm = signedMsg.getView() == 0 || signedMsg.getView() == msg.getNewView();
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), vm, true);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    public static TypedResult<Checkpoint> unpackCheckpoint(SignedMessage signedMsg, KeyStore reg) {
        var typeOk = checkType(signedMsg, PbftMsgTypes.CHECKPOINT);
        if (typeOk != null) return new TypedResult<>(typeOk, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        var val = MessageValidation.validateSigned(signedMsg, RequestType.CHECKPOINT, reg, true);
        if (val.code() != MessageValidation.Code.OK) return new TypedResult<>(val, null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        try {
            Checkpoint msg = Checkpoint.parseFrom(signedMsg.getPayload());
            return new TypedResult<>(MessageValidation.Result.ok(), msg, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), true, true);
        } catch (Exception e) {
            return new TypedResult<>(new MessageValidation.Result(MessageValidation.Code.EMPTY_PAYLOAD, "bad payload: "+e.getMessage()), null, signedMsg.getSignerId(), signedMsg.getView(), signedMsg.getSeq(), false, false);
        }
    }

    private static MessageValidation.Result checkType(SignedMessage signedMsg, String expected) {
        if (!expected.equals(signedMsg.getTypeUrl())) {
            return new MessageValidation.Result(MessageValidation.Code.BAD_TYPEURL,
                    "expected "+expected+" got "+signedMsg.getTypeUrl());
        }
        return null;
    }
}
