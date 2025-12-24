package pbft.replica.net;

import io.grpc.StatusRuntimeException;
import pbft.common.crypto.RequestType;
import pbft.common.validation.SignedMessagePacker;
import pbft.common.validation.PbftMsgTypes;
import pbft.proto.*;
import pbft.replica.rpc.RpcSupport;

import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class ReplicaSender {
    private final RpcSupport.NodeInfo node;
    private final PeerChannels peers;
    private volatile boolean liveEnabled = true;

    private volatile boolean attackByzantine = false;
    private final java.util.Set<String> attackTypes = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.Set<String> attackDarkTargets = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private final java.util.Set<String> attackEquivTargets = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    private volatile long attackTimeDelayMs = 0L;

    public ReplicaSender(RpcSupport.NodeInfo node, PeerChannels peers) {
        this.node = node;
        this.peers = peers;
    }

    public void setLiveEnabled(boolean enabled) { this.liveEnabled = enabled; }

    public void setAttack(boolean byzantine,
                          java.util.Set<String> types,
                          java.util.Set<String> darkTargets,
                          java.util.Set<String> equivTargets,
                          long timeDelayMs) {
        this.attackByzantine = byzantine;
        this.attackTypes.clear(); if (types != null) this.attackTypes.addAll(types);
        this.attackDarkTargets.clear(); if (darkTargets != null) this.attackDarkTargets.addAll(darkTargets);
        this.attackEquivTargets.clear(); if (equivTargets != null) this.attackEquivTargets.addAll(equivTargets);
        this.attackTimeDelayMs = Math.max(0L, timeDelayMs);
    }
    public boolean isByzantine() { return attackByzantine && Boolean.parseBoolean(System.getProperty("pbft.attacks.enabled", "true")); }
    public boolean hasAttackType(String t) { return isByzantine() && attackTypes.contains(t); }
    public java.util.Set<String> equivTargets() { return java.util.Collections.unmodifiableSet(attackEquivTargets); }
    public java.util.Set<String> darkTargets() { return java.util.Collections.unmodifiableSet(attackDarkTargets); }
    public long timeDelayMs() { return attackTimeDelayMs; }

    public Map<String, String> broadcastPrepare(Prepare msg, long deadlineMs) {
        return broadcast(msg, RequestType.PREPARE, PbftMsgTypes.PREPARE, msg.getView(), msg.getSeq(), deadlineMs);
    }

    public Map<String, String> broadcastCommit(Commit msg, long deadlineMs) {
        return broadcast(msg, RequestType.COMMIT, PbftMsgTypes.COMMIT, msg.getView(), msg.getSeq(), deadlineMs);
    }

    public Map<String, String> sendPrePrepareToAll(PrePrepare msg, long deadlineMs) {
        return broadcast(msg, RequestType.PRE_PREPARE, PbftMsgTypes.PRE_PREPARE, msg.getView(), msg.getSeq(), deadlineMs);
    }

    public Map<String, String> broadcastPrepareProof(PrepareProof msg, long deadlineMs) {
        return broadcast(msg, RequestType.PREPARE_PROOF, PbftMsgTypes.PREPARE_PROOF, msg.getView(), msg.getSeq(), deadlineMs);
    }

    public Map<String, String> broadcastCommitProof(CommitProof msg, long deadlineMs) {
        return broadcast(msg, RequestType.COMMIT_PROOF, PbftMsgTypes.COMMIT_PROOF, msg.getView(), msg.getSeq(), deadlineMs);
    }

    public Map<String, String> broadcastViewChange(ViewChange msg, long deadlineMs) {

        return broadcast(msg, RequestType.VIEW_CHANGE, PbftMsgTypes.VIEW_CHANGE, msg.getNewView(), 0, deadlineMs);
    }

    public Map<String, String> sendNewViewToAll(NewView msg, long deadlineMs) {

        return broadcast(msg, RequestType.NEW_VIEW, PbftMsgTypes.NEW_VIEW, msg.getNewView(), 0, deadlineMs);
    }

    public Map<String, String> broadcastCheckpoint(Checkpoint msg, long deadlineMs) {
        return broadcast(msg, RequestType.CHECKPOINT, PbftMsgTypes.CHECKPOINT, 0, msg.getSeq(), deadlineMs);
    }

    public String sendPrepareToPrimary(Prepare msg, long deadlineMs) {
        if (!liveEnabled) return "ERR:SELF";
        if (isByzantine() && hasAttackType("crash")) return "ERR:CRASH";
        String primary = RpcSupport.ViewUtil.primaryId(msg.getView(), node.n);
        try {
            if (isByzantine() && hasAttackType("dark") && attackDarkTargets.contains(primary)) return "ERR:DARK";
            if (!peers.isLive(primary)) return "ERR:DOWN";
            var stub = peers.stubFor(primary).withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
            SignedMessage signedMsg = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, msg, node.nodeId, msg.getView(), msg.getSeq(), node.sk);
            if (isByzantine() && hasAttackType("sign")) {
                var bad = signedMsg.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0})).build();
                stub.sendPrepare(bad);
            } else {
                stub.sendPrepare(signedMsg);
            }
            return "OK";
        } catch (StatusRuntimeException ex) {
            return "ERR:" + ex.getStatus().getCode().name();
        } catch (GeneralSecurityException ex) {
            return "ERR:SIG";
        } catch (RuntimeException ex) {
            return "ERR:DOWN";
        }
    }

    public String sendCommitToPrimary(Commit msg, long deadlineMs) {
        if (!liveEnabled) return "ERR:SELF";
        if (isByzantine() && hasAttackType("crash")) return "ERR:CRASH";
        String primary = RpcSupport.ViewUtil.primaryId(msg.getView(), node.n);
        try {
            if (isByzantine() && hasAttackType("dark") && attackDarkTargets.contains(primary)) return "ERR:DARK";
            if (!peers.isLive(primary)) return "ERR:DOWN";
            var stub = peers.stubFor(primary).withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
            SignedMessage signedMsg = SignedMessagePacker.encode(RequestType.COMMIT, PbftMsgTypes.COMMIT, msg, node.nodeId, msg.getView(), msg.getSeq(), node.sk);
            if (isByzantine() && hasAttackType("sign")) {
                var bad = signedMsg.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0})).build();
                stub.sendCommit(bad);
            } else {
                stub.sendCommit(signedMsg);
            }
            return "OK";
        } catch (StatusRuntimeException ex) {
            return "ERR:" + ex.getStatus().getCode().name();
        } catch (GeneralSecurityException ex) {
            return "ERR:SIG";
        } catch (RuntimeException ex) {
            return "ERR:DOWN";
        }
    }

    public Map<String, String> broadcast(com.google.protobuf.MessageLite msg,
                                          String domain,
                                          String typeUrl,
                                          long envView,
                                          long envSeq,
                                          long deadlineMs) {
        if (!liveEnabled) return java.util.Collections.emptyMap();
        if (isByzantine() && hasAttackType("crash")) {
            return java.util.Collections.emptyMap();
        }
        java.util.Map<String, String> result = new java.util.concurrent.ConcurrentHashMap<>();
        var entries = peers.replicaStubs().entrySet();
        if (isByzantine() && hasAttackType("time")) {
            String primary = RpcSupport.ViewUtil.primaryId(envView, node.n);
            if (node.nodeId.equals(primary)) {
                try { Thread.sleep(Math.max(0L, attackTimeDelayMs)); } catch (InterruptedException ignored) {}
            }
        }

        entries.parallelStream().forEach(e -> {
            var stub = e.getValue().withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
            try {
                if (isByzantine() && hasAttackType("dark") && attackDarkTargets.contains(e.getKey())) {
                    result.put(e.getKey(), "ERR:DARK");
                    return;
                }
                SignedMessage signedMsg = SignedMessagePacker.encode(domain, typeUrl, msg, node.nodeId, envView, envSeq, node.sk);
                if (isByzantine() && hasAttackType("sign")) {
                    var bad = signedMsg.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0})).build();
                    call(stub, typeUrl, bad);
                } else {
                    call(stub, typeUrl, signedMsg);
                }
                result.put(e.getKey(), "OK");
            } catch (StatusRuntimeException ex) {
                result.put(e.getKey(), "ERR:" + ex.getStatus().getCode().name());
            } catch (GeneralSecurityException ex) {
                result.put(e.getKey(), "ERR:SIG");
            } catch (RuntimeException ex) {
                result.put(e.getKey(), "ERR:DOWN");
            }
        });
        return result;
    }

    public Map<String, String> sendPrePrepareToSubset(PrePrepare msg, long deadlineMs, java.util.Set<String> subset) {
        if (!liveEnabled) return java.util.Collections.emptyMap();
        if (isByzantine() && hasAttackType("crash")) return java.util.Collections.emptyMap();
        if (isByzantine() && hasAttackType("time")) {
            String primary = RpcSupport.ViewUtil.primaryId(msg.getView(), node.n);
            if (node.nodeId.equals(primary)) {
                try { Thread.sleep(Math.max(0L, attackTimeDelayMs)); } catch (InterruptedException ignored) {}
            }
        }
        java.util.Map<String, String> result = new java.util.concurrent.ConcurrentHashMap<>();
        for (String id : subset) {
            try {
                if (!peers.isLive(id)) { result.put(id, "ERR:DOWN"); continue; }
                var stub = peers.stubFor(id).withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS);
                if (isByzantine() && hasAttackType("dark") && attackDarkTargets.contains(id)) { result.put(id, "ERR:DARK"); continue; }
                SignedMessage signedMsg = SignedMessagePacker.encode(RequestType.PRE_PREPARE, PbftMsgTypes.PRE_PREPARE, msg, node.nodeId, msg.getView(), msg.getSeq(), node.sk);
                if (isByzantine() && hasAttackType("sign")) {
                    var bad = signedMsg.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0})).build();
                    peers.stubFor(id).sendPrePrepare(bad);
                } else {
                    peers.stubFor(id).sendPrePrepare(signedMsg);
                }
                result.put(id, "OK");
            } catch (StatusRuntimeException ex) {
                result.put(id, "ERR:" + ex.getStatus().getCode().name());
            } catch (GeneralSecurityException ex) {
                result.put(id, "ERR:SIG");
            } catch (RuntimeException ex) {
                result.put(id, "ERR:DOWN");
            }
        }
        return result;
    }

    private void call(ReplicaServiceGrpc.ReplicaServiceBlockingStub stub, String typeUrl, SignedMessage signedMsg) {
        switch (typeUrl) {
            case PbftMsgTypes.PRE_PREPARE -> stub.sendPrePrepare(signedMsg);
            case PbftMsgTypes.PREPARE -> stub.sendPrepare(signedMsg);
            case PbftMsgTypes.COMMIT -> stub.sendCommit(signedMsg);
            case PbftMsgTypes.PREPARE_PROOF -> stub.sendPrepareProof(signedMsg);
            case PbftMsgTypes.COMMIT_PROOF -> stub.sendCommitProof(signedMsg);
            case PbftMsgTypes.VIEW_CHANGE -> stub.sendViewChange(signedMsg);
            case PbftMsgTypes.NEW_VIEW -> stub.sendNewView(signedMsg);
            case PbftMsgTypes.CHECKPOINT -> stub.sendCheckpoint(signedMsg);
            default -> throw new IllegalArgumentException("Unknown typeUrl: " + typeUrl);
        }
    }
}
