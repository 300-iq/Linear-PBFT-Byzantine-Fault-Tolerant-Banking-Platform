package pbft.replica.rpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pbft.common.validation.*;
import io.grpc.Status;
import pbft.proto.*;
import pbft.replica.rpc.RpcSupport.NodeInfo;
import pbft.replica.rpc.RpcSupport.ViewUtil;
import pbft.replica.core.ReplicaContext;
import pbft.replica.core.ReplicaState;
import pbft.replica.core.ConsensusEngine;
import pbft.common.crypto.Digests;
import pbft.common.util.Hex;
import pbft.common.crypto.RequestType;

import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;


public final class ReplicaRpcService extends ReplicaServiceGrpc.ReplicaServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ReplicaRpcService.class);
    private final NodeInfo node;
    private final ConsensusEngine engine;
    private final ReplicaContext ctx;
    private static final long RPC_DEADLINE_MS = Math.max(4000L, Long.getLong("pbft.rpc.deadline_ms", 8000L));
    private static final long CLIENT_REPLY_DEADLINE_MS = Math.max(4000L, Long.getLong("pbft.reply.deadline_ms", RPC_DEADLINE_MS));

    private static final byte[] NULL_DIGEST = pbft.common.crypto.Digests.sha256("pbft:null".getBytes(StandardCharsets.UTF_8));

    public ReplicaRpcService(NodeInfo node, ConsensusEngine engine, ReplicaContext ctx) {
        this.node = node;
        this.engine = engine;
        this.ctx = ctx;
    }

    @Override
    public void sendCheckpoint(SignedMessage env, StreamObserver<Empty> resp) {
        if (!Boolean.getBoolean("pbft.checkpoints.enabled")) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        var tr = MessageDecoder.decodeCheckpoint(env, node.keyStore);
        if (!tr.ok()) {
            resp.onError(Status.INVALID_ARGUMENT.withDescription("bad checkpoint").asRuntimeException());
            return;
        }
        if (!tr.message().getReplicaId().equals(tr.signerId())) {
            resp.onError(Status.INVALID_ARGUMENT.withDescription("replica/signature mismatch").asRuntimeException());
            return;
        }
        if (env.getSeq() != 0 && env.getSeq() != tr.message().getSeq()) {
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header seq mismatch").asRuntimeException());
            return;
        }
        engine.submit("AcceptCheckpoint", () -> {
            long seq = tr.message().getSeq();
            byte[] dg = tr.message().getStateDigest().toByteArray();
            try { ctx.state.recordEvent("IN", "pbft.Checkpoint", 0, seq, tr.signerId(), node.nodeId, "", 0L, dg, "OK", ""); } catch (Exception ignored) {}
            ctx.state.recordCheckpoint(seq, dg, env);
            int count = ctx.state.uniqueCheckpointSignerCount(seq, dg);
            if (count >= (2 * node.f + 1)) {
                if (seq <= ctx.state.lastStableSeq) {
                    if (!ctx.state.matchesStableDigest(seq, dg)) {
                        log.warn("CHKPT s={} digest conflict with stable checkpoint on {}", seq, node.nodeId);
                    }
                    return;
                }
                var cert = ctx.state.checkpointEnvelopes(seq, dg);
                byte[] snap = ctx.state.localCheckpointSnapshot(seq);
                if (snap != null && java.util.Arrays.equals(Digests.sha256(snap), dg)) {
                    ctx.state.setStableCheckpoint(seq, dg, cert);
                    log.info("CHKPT s={} stable ({} signers) on {}", seq, count, node.nodeId);
                    try { executeReady(); } catch (Exception ignored) {}
                } else if (!cert.isEmpty()) {
                    java.util.Set<String> signers = new java.util.HashSet<>();
                    for (SignedMessage sm : cert) {
                        signers.add(sm.getSignerId());
                    }
                    checkpointFetchFromPeers(seq, dg, signers);
                }
            }
        });
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void getCheckpointState(CheckpointQuery req, StreamObserver<CheckpointState> resp) {
        if (!Boolean.getBoolean("pbft.checkpoints.enabled")) {
            resp.onError(Status.UNIMPLEMENTED.withDescription("checkpoints disabled").asRuntimeException());
            return;
        }
        long seq = req.getSeq();
        byte[] expected = req.getStateDigest().toByteArray();
        byte[] snap = ctx.state.localCheckpointSnapshot(seq);
        if (snap == null) {
            resp.onError(Status.NOT_FOUND.withDescription("snapshot unavailable").asRuntimeException());
            return;
        }
        byte[] calc = Digests.sha256(snap);
        if (!java.util.Arrays.equals(calc, expected)) {
            resp.onError(Status.DATA_LOSS.withDescription("digest mismatch").asRuntimeException());
            return;
        }
        CheckpointState reply = CheckpointState.newBuilder()
                .setSeq(seq)
                .setStateDigest(ByteString.copyFrom(expected))
                .setPayload(ByteString.copyFrom(snap))
                .build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    private void checkpointFetchFromPeers(long seq, byte[] digest, java.util.Set<String> signers) {
        if (signers == null || signers.isEmpty() || digest == null) return;
        byte[] localSnap = ctx.state.localCheckpointSnapshot(seq);
        if (localSnap != null && java.util.Arrays.equals(Digests.sha256(localSnap), digest)) {
            ctx.state.setStableCheckpoint(seq, digest, ctx.state.checkpointEnvelopes(seq, digest));
            ctx.state.clearCheckpointFetch(seq, digest);
            try { executeReady(); } catch (Exception ignored) {}
            return;
        }
        if (!ctx.state.markCheckpointFetch(seq, digest)) {
            return;
        }
        ctx.outbound.execute(() -> {
            for (String id : signers) {
                try {
                    var stub = ctx.peers.stubFor(id).withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                    CheckpointQuery q = CheckpointQuery.newBuilder().setSeq(seq).setStateDigest(ByteString.copyFrom(digest)).build();
                    CheckpointState st = stub.getCheckpointState(q);
                    if (st.getSeq() != seq) continue;
                    byte[] sd = st.getStateDigest().toByteArray();
                    if (!java.util.Arrays.equals(sd, digest)) continue;
                    byte[] payload = st.getPayload().toByteArray();
                    if (!java.util.Arrays.equals(Digests.sha256(payload), digest)) continue;
                    engine.submit("InstallCheckpoint (" + seq + ")", () -> {
                        try {
                            ctx.db.restoreFromCanonical(payload);
                        } catch (Exception ex) {
                            ctx.state.clearCheckpointFetch(seq, digest);
                            log.error("CHKPT s={} restore failed on {}: {}", seq, node.nodeId, ex.getMessage());
                            return;
                        }
                        try {
                            Checkpoint cp = Checkpoint.newBuilder().setSeq(seq).setStateDigest(ByteString.copyFrom(digest)).setReplicaId(node.nodeId).build();
                            SignedMessage env = SignedMessagePacker.encode(RequestType.CHECKPOINT, PbftMsgTypes.CHECKPOINT, cp, node.nodeId, 0, seq, node.sk);
                            ctx.state.recordLocalCheckpoint(seq, digest, env, payload);
                            ctx.state.setStableCheckpoint(seq, digest, ctx.state.checkpointEnvelopes(seq, digest));
                            ctx.state.clearCheckpointFetch(seq, digest);
                            try { executeReady(); } catch (Exception ignored) {}
                        } catch (Exception ignore) {
                            ctx.state.clearCheckpointFetch(seq, digest);
                        }
                    });
                    return;
                } catch (Exception ignore) {
                }
            }
            log.warn("CHKPT s={} fetch failed from certificate signers on {}", seq, node.nodeId);
            if (Boolean.getBoolean("pbft.checkpoints.fallbackAllPeers")) {
                for (var e : ctx.peers.replicaStubs().entrySet()) {
                    if (signers.contains(e.getKey())) continue;
                    try {
                        var stub = e.getValue().withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                        CheckpointQuery q = CheckpointQuery.newBuilder().setSeq(seq).setStateDigest(ByteString.copyFrom(digest)).build();
                        CheckpointState st = stub.getCheckpointState(q);
                        if (st.getSeq() != seq) continue;
                        byte[] sd = st.getStateDigest().toByteArray();
                        if (!java.util.Arrays.equals(sd, digest)) continue;
                        byte[] payload = st.getPayload().toByteArray();
                        if (!java.util.Arrays.equals(Digests.sha256(payload), digest)) continue;
                        engine.submit("InstallCheckpoint (" + seq + ")", () -> {
                            try {
                                ctx.db.restoreFromCanonical(payload);
                            } catch (Exception ex) {
                                ctx.state.clearCheckpointFetch(seq, digest);
                                log.error("CHKPT s={} restore failed on {}: {}", seq, node.nodeId, ex.getMessage());
                                return;
                            }
                            try {
                                Checkpoint cp = Checkpoint.newBuilder().setSeq(seq).setStateDigest(ByteString.copyFrom(digest)).setReplicaId(node.nodeId).build();
                                SignedMessage env = SignedMessagePacker.encode(RequestType.CHECKPOINT, PbftMsgTypes.CHECKPOINT, cp, node.nodeId, 0, seq, node.sk);
                                ctx.state.recordLocalCheckpoint(seq, digest, env, payload);
                                ctx.state.setStableCheckpoint(seq, digest, ctx.state.checkpointEnvelopes(seq, digest));
                                ctx.state.clearCheckpointFetch(seq, digest);
                                try { executeReady(); } catch (Exception ignored) {}
                            } catch (Exception ignore) {
                                ctx.state.clearCheckpointFetch(seq, digest);
                            }
                        });
                        return;
                    } catch (Exception ignore) {
                    }
                }
                log.warn("CHKPT s={} fallback fetch failed on {}", seq, node.nodeId);
            }
            ctx.state.clearCheckpointFetch(seq, digest);
        });
    }

    private void buildAndBroadcastNewView(long vPrime, List<SignedMessage> vcEnvs) {
        if (node.nodeId.equals(ViewUtil.primaryId(vPrime, node.n))
                && ctx.sender.isByzantine()
                && (ctx.sender.hasAttackType("crash") || ctx.sender.hasAttackType("failstop"))) {
            String mode = ctx.sender.hasAttackType("failstop") ? "fail-stop" : "crash";
            log.info("NV v'={} primary {}: suppress NewView on {}", vPrime, mode, node.nodeId);
            return;
        }
        List<SignedMessage> chosenVCs = new ArrayList<>();
        SignedMessage self = null;
        for (var env : vcEnvs) {
            if (env.getSignerId().equals(node.nodeId)) {
                self = env;
                break;
            }
        }
        if (self != null) {
            chosenVCs.add(self);
        }
        for (var env : vcEnvs) {
            if (chosenVCs.size() >= (2 * node.f + 1)) break;
            if (self != null && env == self) continue;
            chosenVCs.add(env);
        }
        java.util.HashSet<String> signers = new java.util.HashSet<>();
        for (SignedMessage env : chosenVCs) {
            var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
            if (!vctr.ok()) {
                log.warn("DROP VC in NV build: bad envelope from {}", env.getSignerId());
                return;
            }
            if (vctr.message().getNewView() != vPrime) {
                log.warn("DROP VC in NV build: wrong v' in VC from {}", env.getSignerId());
                return;
            }
            if (!vctr.signerId().equals(vctr.message().getReplicaId())) {
                log.warn("DROP VC in NV build: signer/replica mismatch {}", env.getSignerId());
                return;
            }
            signers.add(vctr.signerId());
        }
        if (signers.size() < (2 * node.f + 1)) {
            log.warn("Abort NV build v'={} due to insufficient distinct VC signers: {}", vPrime, signers.size());
            return;
        }

        long s = 0L;
        Map<Long, PreparedEntry> bestBySeq = new HashMap<>();
        for (SignedMessage env : chosenVCs) {
            var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
            if (!vctr.ok()) continue;
            s = Math.max(s, vctr.message().getLastStableSeq());
            for (PreparedEntry pe : vctr.message().getPsetList()) {
                if (pe.getSeq() <= s) continue;
                if (!pe.hasProof()) continue;
                var pv = ProofValidator.validatePrepareProof(pe.getProof(), node.keyStore, node.n, node.f);
                if (!pv.ok()) continue;
                PreparedEntry cur = bestBySeq.get(pe.getSeq());
                if (cur == null || pe.getView() > cur.getView()) {
                    bestBySeq.put(pe.getSeq(), pe);
                }
            }
        }
        long h = 0L;
        for (var e : bestBySeq.keySet()) if (e > h) h = e;
        List<PrePrepare> selected = new ArrayList<>();
        long maxSeq = h;
        for (long seq = s + 1; seq <= h; seq++) {
            PreparedEntry pe = bestBySeq.get(seq);
            if (pe != null) {
                PrePrepare.Builder pp = PrePrepare.newBuilder()
                        .setView(vPrime)
                        .setSeq(seq)
                        .setRequestDigest(pe.getRequestDigest())
                        .setClientId(pe.getClientId())
                        .setReqSeq(pe.getReqSeq());
                var oldEntry = ctx.state.get(pe.getView(), pe.getSeq());
                if (oldEntry != null && oldEntry.clientRequestSignedMsg != null) {
                    pp.setClientRequest(oldEntry.clientRequestSignedMsg);
                } else {
                    var prevKey = ctx.state.findByClientReq(pe.getClientId(), pe.getReqSeq());
                    if (prevKey != null) {
                        var prev = ctx.state.get(prevKey.view, prevKey.seq);
                        if (prev != null && prev.clientRequestSignedMsg != null) {
                            pp.setClientRequest(prev.clientRequestSignedMsg);
                        }
                    }
                }
                PrePrepare built = pp.build();
                selected.add(built);
                var newE = ctx.state.getOrCreate(vPrime, seq);
                ReplicaState.setOrVerifyDigest(newE, pe.getRequestDigest().toByteArray());
                newE.prePrepare = built;
                newE.prePrepared = true;
                if (built.hasClientRequest()) {
                    var crTr = MessageDecoder.decodeClientRequest(built.getClientRequest(), node.keyStore);
                    if (crTr.ok()) {
                        newE.clientRequest = crTr.message();
                        newE.clientRequestSignedMsg = built.getClientRequest();
                    }
                }
                if (!pe.getClientId().isEmpty() && pe.getReqSeq() != 0L) {
                    ctx.state.indexClientReq(pe.getClientId(), pe.getReqSeq(), new ReplicaState.EntryKey(vPrime, seq));
                }
                try {
                    Prepare selfPr = Prepare.newBuilder().setView(vPrime).setSeq(seq).setRequestDigest(pe.getRequestDigest()).setReplicaId(node.nodeId).build();
                    SignedMessage prEnv = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, selfPr, node.nodeId, vPrime, seq, node.sk);
                    if (newE.prepares.add(node.nodeId)) {
                        newE.prepareSignedMsgs.put(node.nodeId, prEnv);
                    }
                } catch (Exception ignored) {
                }
            } else {
                PrePrepare built = PrePrepare.newBuilder()
                        .setView(vPrime)
                        .setSeq(seq)
                        .setRequestDigest(ByteString.copyFrom(NULL_DIGEST))
                        .build();
                selected.add(built);
                var newE = ctx.state.getOrCreate(vPrime, seq);
                ReplicaState.setOrVerifyDigest(newE, NULL_DIGEST);
                newE.prePrepare = built;
                newE.prePrepared = true;
                try {
                    Prepare selfPr = Prepare.newBuilder().setView(vPrime).setSeq(seq).setRequestDigest(ByteString.copyFrom(NULL_DIGEST)).setReplicaId(node.nodeId).build();
                    SignedMessage prEnv = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, selfPr, node.nodeId, vPrime, seq, node.sk);
                    if (newE.prepares.add(node.nodeId)) {
                        newE.prepareSignedMsgs.put(node.nodeId, prEnv);
                    }
                } catch (Exception ignored) {
                }
            }
        }
        selected.sort(java.util.Comparator.comparingLong(PrePrepare::getSeq));
        chosenVCs.sort(java.util.Comparator.comparing(pbft.proto.SignedMessage::getSignerId));
        NewView nv = NewView.newBuilder().setNewView(vPrime).addAllViewChanges(chosenVCs).addAllSelectedPreprepares(selected).build();
        ctx.state.setCurrentView(vPrime);
        ctx.state.viewChangeActive = false;
        ctx.state.pendingNewView = 0;
        if (maxSeq > 0) ctx.state.setNextSeq(maxSeq + 1);
        ctx.timers.onNewViewAccepted(vPrime);
        ctx.state.markNewViewSent(vPrime);
        ctx.state.setLastNewView(nv, s, h);
        ctx.state.appendNewViewHistory(nv, s, h);
        ctx.state.pruneViewChangesUpTo(vPrime);
        ctx.outbound.execute(() -> {
            var res = ctx.sender.sendNewViewToAll(nv, RPC_DEADLINE_MS);
            logSendResults("NewView", vPrime, 0, res);
            try { recordOutSendResults("NewView", vPrime, 0, res); } catch (Exception ignored) {}
        });
    }

    private boolean validateNewView(NewView nv) {
        long vPrime = nv.getNewView();
        if (nv.getViewChangesCount() < (2 * node.f + 1)) return false;
        long s = 0L;
        Map<Long, PreparedEntry> bestBySeq = new HashMap<>();
        java.util.Set<String> vcSigners = new java.util.HashSet<>();
        for (SignedMessage env : nv.getViewChangesList()) {
            var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
            if (!vctr.ok()) return false;
            if (vctr.message().getNewView() != vPrime) return false;
            if (!vctr.signerId().equals(vctr.message().getReplicaId())) return false;
            vcSigners.add(vctr.signerId());
            s = Math.max(s, vctr.message().getLastStableSeq());
            for (PreparedEntry pe : vctr.message().getPsetList()) {
                if (pe.getSeq() <= s) continue;
                if (!pe.hasProof()) return false;
                var pv = ProofValidator.validatePrepareProof(pe.getProof(), node.keyStore, node.n, node.f);
                if (!pv.ok()) return false;
                if (pe.getProof().getView() != pe.getView() || pe.getProof().getSeq() != pe.getSeq() || !pe.getProof().getRequestDigest().equals(pe.getRequestDigest()))
                    return false;
                PreparedEntry cur = bestBySeq.get(pe.getSeq());
                if (cur == null || pe.getView() > cur.getView()) bestBySeq.put(pe.getSeq(), pe);
            }
        }
        if (vcSigners.size() < (2 * node.f + 1)) return false;
        if (Boolean.getBoolean("pbft.checkpoints.enabled") && s > 0) {
            java.util.Map<String, java.util.Set<String>> byDigestSigners = new java.util.HashMap<>();
            for (SignedMessage env : nv.getViewChangesList()) {
                var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
                if (!vctr.ok()) return false;
                for (SignedMessage cenv : vctr.message().getCheckpointMsgsList()) {
                    var cTr = MessageDecoder.decodeCheckpoint(cenv, node.keyStore);
                    if (!cTr.ok()) return false;
                    if (cTr.message().getSeq() != s) continue;
                    String dHex = Hex.toHexOrEmpty(cTr.message().getStateDigest().toByteArray());
                    byDigestSigners.computeIfAbsent(dHex, _k -> new java.util.HashSet<>()).add(cTr.signerId());
                }
            }
            boolean certOk = false;
            for (var e : byDigestSigners.entrySet()) {
                if (e.getValue().size() >= (2 * node.f + 1)) {
                    certOk = true;
                    break;
                }
            }
            if (!certOk) return false;
            for (SignedMessage env : nv.getViewChangesList()) {
                var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
                if (!vctr.ok()) return false;
                long sVc = vctr.message().getLastStableSeq();
                if (sVc <= 0) continue;
                com.google.protobuf.ByteString declared = vctr.message().getCheckpointDigest();
                if (declared == null || declared.isEmpty()) continue;
                boolean found = false;
                for (SignedMessage cenv : vctr.message().getCheckpointMsgsList()) {
                    var cTr = MessageDecoder.decodeCheckpoint(cenv, node.keyStore);
                    if (!cTr.ok()) return false;
                    if (cTr.message().getSeq() != sVc) continue;
                    if (cTr.message().getStateDigest().equals(declared)) { found = true; break; }
                }
                if (!found) return false;
            }
        }
        Map<Long, ByteString> expectedBySeq = new HashMap<>();
        for (var e : bestBySeq.entrySet()) {
            expectedBySeq.put(e.getKey(), e.getValue().getRequestDigest());
        }
        Map<Long, ByteString> nvBySeq = new HashMap<>();
        for (PrePrepare pp : nv.getSelectedPrepreparesList()) {
            if (pp.getView() != vPrime) return false;
            if (pp.getSeq() <= s) return false;
            if (pp.hasClientRequest()) {
                var crTr = MessageDecoder.decodeClientRequest(pp.getClientRequest(), node.keyStore);
                if (!crTr.ok()) return false;
                byte[] calc = Digests.sha256(crTr.message().toByteArray());
                if (!java.util.Arrays.equals(calc, pp.getRequestDigest().toByteArray())) return false;
            }
            nvBySeq.put(pp.getSeq(), pp.getRequestDigest());
        }
        long h = 0L;
        for (var k : expectedBySeq.keySet()) if (k > h) h = k;
        if (nvBySeq.size() != Math.max(0, (int) (h - s))) return false;
        for (long seq = s + 1; seq <= h; seq++) {
            ByteString nvDigest = nvBySeq.get(seq);
            if (nvDigest == null) return false;
            ByteString expected = expectedBySeq.get(seq);
            if (expected != null) {
                if (!nvDigest.equals(expected)) return false;
            } else {
                if (!nvDigest.equals(ByteString.copyFrom(NULL_DIGEST))) return false;
            }
        }
        return true;
    }

    private void acceptNewView(NewView nv) {
        long vPrime = nv.getNewView();
        ctx.state.setCurrentView(vPrime);
        ctx.state.viewChangeActive = false;
        ctx.state.pendingNewView = 0;
        if (ctx.sender.isByzantine() && ctx.sender.hasAttackType("crash")) {
            ctx.timers.onNewViewAccepted(vPrime);
            return;
        }
        for (PrePrepare pp : nv.getSelectedPrepreparesList()) {
            long s = pp.getSeq();
            var e = ctx.state.getOrCreate(vPrime, s);
            if (!ReplicaState.setOrVerifyDigest(e, pp.getRequestDigest().toByteArray())) continue;
            if (!pp.getClientId().isEmpty() && pp.getReqSeq() != 0L) {
                ctx.state.indexClientReq(pp.getClientId(), pp.getReqSeq(), new ReplicaState.EntryKey(vPrime, s));
            }
            e.prePrepare = pp;
            e.prePrepared = true;
            if (pp.hasClientRequest()) {
                var crTr = MessageDecoder.decodeClientRequest(pp.getClientRequest(), ctx.node.keyStore);
                if (crTr.ok()) {
                    e.clientRequest = crTr.message();
                    e.clientRequestSignedMsg = pp.getClientRequest();
                }
            } else {
                var prevKey = ctx.state.findByClientReq(pp.getClientId(), pp.getReqSeq());
                if (prevKey != null) {
                    var prev = ctx.state.get(prevKey.view, prevKey.seq);
                    if (prev != null && prev.clientRequestSignedMsg != null) {
                        e.clientRequest = prev.clientRequest;
                        e.clientRequestSignedMsg = prev.clientRequestSignedMsg;
                    }
                }
                if (e.clientRequest == null && e.requestDigest != null && !e.requestFetchInProgress) {
                    e.requestFetchInProgress = true;
                    requestFetchFromPeers(vPrime, s, e.requestDigest, pp.getClientId(), pp.getReqSeq());
                }
            }
            if (!node.nodeId.equals(ViewUtil.primaryId(vPrime, node.n))) {
                ctx.timers.onPrePrepareAccepted(vPrime, s);
            }
            Prepare pr = Prepare.newBuilder().setView(vPrime).setSeq(s).setRequestDigest(pp.getRequestDigest()).setReplicaId(node.nodeId).build();
            try {
                SignedMessage prEnv = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, pr, node.nodeId, vPrime, s, node.sk);
                if (e.prepares.add(node.nodeId)) {
                    e.prepareSignedMsgs.put(node.nodeId, prEnv);
                }
            } catch (Exception ignored) {
            }
            ctx.outbound.execute(() -> {
                String status = ctx.sender.sendPrepareToPrimary(pr, RPC_DEADLINE_MS);
                if (!"OK".equals(status)) {
                    if (Boolean.getBoolean("pbft.log.sendDetails")) {
                        log.warn("SEND Prepare (NV) v'={} s={} from={} status={} on {}", vPrime, s, node.nodeId, status, node.nodeId);
                    } else {
                        log.debug("SEND Prepare (NV) v'={} s={} from={} status={} on {}", vPrime, s, node.nodeId, status, node.nodeId);
                    }
                }
            });
        }
        ctx.timers.onNewViewAccepted(vPrime);
        long sObs = 0L;
        for (SignedMessage env : nv.getViewChangesList()) {
            try {
                var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
                if (vctr.ok()) sObs = Math.max(sObs, vctr.message().getLastStableSeq());
            } catch (Exception ignored) {
            }
        }
        long hObs = 0L;
        for (PrePrepare pp : nv.getSelectedPrepreparesList()) {
            if (pp.getSeq() > hObs) hObs = pp.getSeq();
        }
        ctx.state.setLastNewView(nv, sObs, hObs);
        ctx.state.appendNewViewHistory(nv, sObs, hObs);
        ctx.state.pruneViewChangesUpTo(vPrime);
        if (Boolean.getBoolean("pbft.checkpoints.enabled") && sObs > ctx.state.lastStableSeq) {
            java.util.Map<String, java.util.Set<String>> byDigestSigners = new java.util.HashMap<>();
            java.util.Map<String, java.util.List<SignedMessage>> byDigestCerts = new java.util.HashMap<>();
            for (SignedMessage env : nv.getViewChangesList()) {
                var vctr = MessageDecoder.decodeViewChange(env, node.keyStore);
                if (!vctr.ok()) continue;
                for (SignedMessage cenv : vctr.message().getCheckpointMsgsList()) {
                    var cTr = MessageDecoder.decodeCheckpoint(cenv, node.keyStore);
                    if (!cTr.ok()) continue;
                    if (cTr.message().getSeq() != sObs) continue;
                    if (!cTr.message().getReplicaId().equals(cTr.signerId())) return;
                    byte[] digestBytes = cTr.message().getStateDigest().toByteArray();
                    String dHex = Hex.toHexOrEmpty(digestBytes);
                    byDigestSigners.computeIfAbsent(dHex, _k -> new java.util.HashSet<>()).add(cTr.signerId());
                    byDigestCerts.computeIfAbsent(dHex, _k -> new java.util.ArrayList<>()).add(cenv);
                    ctx.state.recordCheckpoint(sObs, digestBytes, cenv);
                }
            }
            String chosenHex = null;
            java.util.Set<String> chosenSigners = null;
            for (var e : byDigestSigners.entrySet()) {
                if (e.getValue().size() >= (2 * node.f + 1)) {
                    chosenHex = e.getKey();
                    chosenSigners = e.getValue();
                    break;
                }
            }
            if (chosenHex != null) {
                byte[] digest = pbft.common.util.Hex.fromHex(chosenHex);
                byte[] local = ctx.state.localCheckpointSnapshot(sObs);
                var chosenCertRaw = byDigestCerts.getOrDefault(chosenHex, java.util.Collections.emptyList());
                java.util.Map<String, SignedMessage> unique = new java.util.LinkedHashMap<>();
                for (SignedMessage sm : chosenCertRaw) {
                    unique.putIfAbsent(sm.getSignerId(), sm);
                }
                var chosenCert = new java.util.ArrayList<>(unique.values());
                if (local != null && java.util.Arrays.equals(Digests.sha256(local), digest)) {
                    ctx.state.setStableCheckpoint(sObs, digest, chosenCert);
                    try { executeReady(); } catch (Exception ignored) {}
                } else {
                    checkpointFetchFromPeers(sObs, digest, chosenSigners);
                }
            }
        }
        try {
            java.util.List<String> sels = new java.util.ArrayList<>();
            for (PrePrepare pp : nv.getSelectedPrepreparesList()) {
                String dg = pbft.common.util.Hex.toHexOrEmpty(pp.getRequestDigest().toByteArray());
                if (dg.length() > 8) dg = dg.substring(0, 8);
                sels.add(pp.getSeq() + ":" + dg);
            }
            String primaryId = ViewUtil.primaryId(vPrime, node.n);
            String line = "Accept NewView v'=" + vPrime + " primary=" + primaryId + " selected=" + sels;
            ctx.state.appendViewLog(line);
        } catch (Exception ignored) {
        }
    }


    @Override
    public void sendPrePrepare(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodePrePrepare(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT PrePrepare v={} s={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        long v = tr.message().getView();
        if (ctx.state.viewChangeActive && v == ctx.state.currentView()) {
            ctx.state.viewChangeActive = false;
            ctx.state.pendingNewView = 0;
            ctx.timers.onViewStart(v);
        }
        String expectedPrimary = ViewUtil.primaryId(v, node.n);
        if (!ctx.peers.isLive(tr.signerId()) && !tr.signerId().equals(expectedPrimary)) {
            log.warn("DROP PrePrepare from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }

        long s = tr.message().getSeq();
        if (ctx.sender.isByzantine() && ctx.sender.hasAttackType("crash")) {
            log.info("PP v={} s={} suppress accept on crash on {}", v, s, node.nodeId);
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        if (!tr.viewMatches() || !tr.seqMatches()) {
            log.warn("REJECT PrePrepare v={} s={} signer={} reason=signed-message/payload mismatch on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view/seq mismatch").asRuntimeException());
            return;
        }
        boolean roleOk = tr.signerId().equals(ViewUtil.primaryId(v, node.n));
        if (!roleOk) {
            log.warn("REJECT PrePrepare v={} s={} signer={} reason=role violation (primary={}) on {}", v, s, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("role violation: only primary may send PrePrepare").asRuntimeException());
            return;
        }
        long currentView = ctx.state.currentView();
        if (v != currentView) {
            log.warn("REJECT PrePrepare v={} s={} signer={} reason=unexpected view current={} on {}", v, s, tr.signerId(), currentView, node.nodeId);
            resp.onError(Status.FAILED_PRECONDITION.withDescription("unexpected view").asRuntimeException());
            return;
        }
        byte[] digestBytes = tr.message().getRequestDigest().toByteArray();
        var existing = ctx.state.get(v, s);
        if (existing != null && existing.requestDigest != null && !Arrays.equals(existing.requestDigest, digestBytes)) {
            log.warn("REJECT PrePrepare v={} s={} signer={} reason=digest conflict existing={} new={} on {}",
                    v, s, tr.signerId(), Hex.toHexOrNullLiteral(existing.requestDigest), Hex.toHexOrNullLiteral(digestBytes), node.nodeId);
            resp.onError(Status.ALREADY_EXISTS.withDescription("conflicting PrePrepare digest for seq").asRuntimeException());
            return;
        }
        if (tr.message().hasClientRequest()) {
            var crTr = MessageDecoder.decodeClientRequest(tr.message().getClientRequest(), node.keyStore);
            if (!crTr.ok()) {
                log.warn("REJECT PrePrepare v={} s={} signer={} reason=bad embedded ClientRequest: {} on {}", v, s, tr.signerId(), crTr.validation(), node.nodeId);
                resp.onError(GrpcStatusUtil.mapStatus(crTr.validation()).asRuntimeException());
                return;
            }
            byte[] calc = Digests.sha256(crTr.message().toByteArray());
            if (!Arrays.equals(calc, digestBytes)) {
                log.warn("REJECT PrePrepare v={} s={} signer={} reason=digest mismatch on {}", v, s, tr.signerId(), node.nodeId);
                resp.onError(Status.INVALID_ARGUMENT.withDescription("preprepare client_request digest mismatch").asRuntimeException());
                return;
            }
            if (!tr.message().getClientId().equals(crTr.message().getClientId()) || tr.message().getReqSeq() != crTr.message().getReqSeq()) {
                log.warn("REJECT PrePrepare v={} s={} signer={} reason=client_id/req_seq mismatch vs embedded request on {}", v, s, tr.signerId(), node.nodeId);
                resp.onError(Status.INVALID_ARGUMENT.withDescription("preprepare client_id/req_seq mismatch vs embedded request").asRuntimeException());
                return;
            }
        }
        if (!tr.message().getClientId().isEmpty() && tr.message().getReqSeq() != 0L) {
            if (ctx.state.hasExecuted(tr.message().getClientId(), tr.message().getReqSeq())) {
                log.debug("IGNORE PrePrepare v={} s={} for already executed {}#{} on {}", v, s, tr.message().getClientId(), tr.message().getReqSeq(), node.nodeId);
                resp.onNext(Empty.getDefaultInstance());
                resp.onCompleted();
                return;
            }
        }
        try { ctx.state.recordEvent("IN", "pbft.PrePrepare", v, s, tr.signerId(), node.nodeId, tr.message().getClientId(), tr.message().getReqSeq(), tr.message().getRequestDigest().toByteArray(), "OK", ""); } catch (Exception ignored) {}
        engine.submit("PrePrepare (" + v + "," + s + ")", () -> {
            var e = ctx.state.getOrCreate(v, s);
            if (!ReplicaState.setOrVerifyDigest(e, digestBytes)) {
                log.warn("IGNORE PrePrepare v={} s={} on {} due to digest mismatch with existing entry", v, s, node.nodeId);
                return;
            }
            e.prePrepare = tr.message();
            e.prePrepared = true;
            if (e.clientRequest == null && !tr.message().hasClientRequest()) {
                if (!e.requestFetchInProgress && e.requestDigest != null) {
                    e.requestFetchInProgress = true;
                    requestFetchFromPeers(v, s, e.requestDigest, tr.message().getClientId(), tr.message().getReqSeq());
                }
            }
            ctx.timers.onPrePrepareAccepted(v, s);
            ctx.state.indexClientReq(tr.message().getClientId(), tr.message().getReqSeq(), new ReplicaState.EntryKey(v, s));
            String dig = Hex.toHexOrEmpty(digestBytes);
            if (dig.length() > 16) dig = dig.substring(0, 16);
            log.info("PP v={} s={} client={}#{} t={} dig={} on {}", v, s, tr.message().getClientId(), tr.message().getReqSeq(), tr.message().getReqSeq(), dig, node.nodeId);
            if (e.clientRequest == null && tr.message().hasClientRequest()) {
                var crSignedMsg = tr.message().getClientRequest();
                var crTr = MessageDecoder.decodeClientRequest(crSignedMsg, node.keyStore);
                if (crTr.ok()) {
                    e.clientRequestSignedMsg = crSignedMsg;
                    e.clientRequest = crTr.message();
                }
            }
            if (!node.nodeId.equals(ViewUtil.primaryId(v, node.n))) {
                if (ctx.sender.isByzantine() && (ctx.sender.hasAttackType("crash") || ctx.sender.hasAttackType("failstop"))) {
                    String mode2 = ctx.sender.hasAttackType("failstop") ? "fail-stop" : "crash";
                    log.info("P  v={} s={} backup {}: suppress Prepare on {}", v, s, mode2, node.nodeId);
                } else {
                    Prepare pr = Prepare.newBuilder()
                            .setView(v).setSeq(s)
                            .setRequestDigest(tr.message().getRequestDigest())
                            .setReplicaId(node.nodeId)
                            .build();
                    try {
                        SignedMessage prSignedMsg = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, pr, node.nodeId, v, s, node.sk);
                        if (e.prepares.add(node.nodeId)) {
                            e.prepareSignedMsgs.put(node.nodeId, prSignedMsg);
                        }
                    } catch (Exception ignore) {
                    }
                    ctx.outbound.execute(() -> {
                        String status = ctx.sender.sendPrepareToPrimary(pr, RPC_DEADLINE_MS);
                        if (!"OK".equals(status)) {
                            if (Boolean.getBoolean("pbft.log.sendDetails")) {
                                log.warn("SEND Prepare v={} s={} from={} status={} on {}", v, s, node.nodeId, status, node.nodeId);
                            } else {
                                log.debug("SEND Prepare v={} s={} from={} status={} on {}", v, s, node.nodeId, status, node.nodeId);
                            }
                        }
                        try { ctx.state.recordEvent("OUT", "pbft.Prepare", v, s, node.nodeId, ViewUtil.primaryId(v, node.n), "", 0L, pr.getRequestDigest().toByteArray(), status, ""); } catch (Exception ignored) {}
                    });
                }
            }
            executeReady();
        });
        log.debug("ACCEPT PrePrepare v={} s={} signer={} primary={} on {}", v, s, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void sendPrepare(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodePrepare(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT Prepare v={} s={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        if (!tr.viewMatches() || !tr.seqMatches()) {
            log.warn("REJECT Prepare v={} s={} signer={} reason=signed-message/payload mismatch on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view/seq mismatch").asRuntimeException());
            return;
        }
        if (!ctx.peers.isLive(tr.signerId())) {
            log.warn("DROP Prepare from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }
        long v = tr.message().getView();
        long s = tr.message().getSeq();
        if (ctx.state.viewChangeActive && v == ctx.state.currentView()) {
            ctx.state.viewChangeActive = false;
            ctx.state.pendingNewView = 0;
            ctx.timers.onViewStart(v);
        }
        long currentView = ctx.state.currentView();
        if (v != currentView) {
            log.warn("REJECT Prepare v={} s={} signer={} reason=unexpected view current={} on {}", v, s, tr.signerId(), currentView, node.nodeId);
            resp.onError(Status.FAILED_PRECONDITION.withDescription("unexpected view").asRuntimeException());
            return;
        }
        String primaryId = ViewUtil.primaryId(v, node.n);
        if (!node.nodeId.equals(primaryId)) {
            log.warn("REJECT Prepare v={} s={} signer={} reason=role violation (receiver not primary, primary={}) on {}", v, s, tr.signerId(), primaryId, node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("role violation: only primary may receive Prepare").asRuntimeException());
            return;
        }
        if (!tr.signerId().equals(tr.message().getReplicaId())) {
            log.warn("REJECT Prepare v={} s={} signer={} reason=replicaId mismatch inner={} on {}", v, s, tr.signerId(), tr.message().getReplicaId(), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("replicaId/signature mismatch").asRuntimeException());
            return;
        }
        byte[] digestBytes = tr.message().getRequestDigest().toByteArray();
        var existing = ctx.state.get(v, s);
        if (existing != null && existing.requestDigest != null && !Arrays.equals(existing.requestDigest, digestBytes)) {
            log.warn("REJECT Prepare v={} s={} signer={} reason=digest mismatch existing={} new={} on {}",
                    v, s, tr.signerId(), Hex.toHexOrNullLiteral(existing.requestDigest), Hex.toHexOrNullLiteral(digestBytes), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("prepare digest mismatch").asRuntimeException());
            return;
        }
        try { ctx.state.recordEvent("IN", "pbft.Prepare", v, s, tr.signerId(), node.nodeId, "", 0L, tr.message().getRequestDigest().toByteArray(), "OK", ""); } catch (Exception ignored) {}
        engine.submit("Prepare (" + v + "," + s + ")", () -> {
            var e = ctx.state.getOrCreate(v, s);
            if (!ReplicaState.setOrVerifyDigest(e, digestBytes)) {
                log.warn("IGNORE Prepare v={} s={} signer={} due to digest conflict on {}", v, s, tr.signerId(), node.nodeId);
                return;
            }
            if (e.prepares.add(tr.signerId())) {
                e.prepareSignedMsgs.put(tr.signerId(), signedMsg);
            }
            int threshold = node.n - node.f;
            if (e.prepares.size() >= threshold && !e.prepareProofSent) {
                if (ctx.sender.isByzantine() && (ctx.sender.hasAttackType("crash") || ctx.sender.hasAttackType("failstop"))) {
                    String mode = ctx.sender.hasAttackType("failstop") ? "fail-stop" : "crash";
                    log.info("P  v={} s={} primary {}: suppress PrepareProof on {}", v, s, mode, node.nodeId);
                    return;
                }
                PrepareProof proof = PrepareProof.newBuilder()
                        .setView(v)
                        .setSeq(s)
                        .setRequestDigest(com.google.protobuf.ByteString.copyFrom(e.requestDigest))
                        .addAllSignedPrepares(e.prepareSignedMsgs.values())
                        .build();
                e.prepareProofSent = true;
                e.prepareProof = proof;
                e.prepared = true;
                ctx.timers.pulse("primary-prepare-quorum");
                log.info("P  v={} s={} prepared ({} prepares) primary={} on {}", v, s, e.prepares.size(), ViewUtil.primaryId(v, node.n), node.nodeId);
                ctx.outbound.execute(() -> {
                    var res = ctx.sender.broadcastPrepareProof(proof, RPC_DEADLINE_MS);
                    logSendResults("PrepareProof", v, s, res);
                    try { recordOutSendResults("PrepareProof", v, s, res); } catch (Exception ignored) {}
                });
                Commit selfCommit = Commit.newBuilder()
                        .setView(v).setSeq(s)
                        .setRequestDigest(proof.getRequestDigest())
                        .setReplicaId(node.nodeId)
                        .build();
                try {
                    SignedMessage cEnv = SignedMessagePacker.encode(RequestType.COMMIT, PbftMsgTypes.COMMIT, selfCommit, node.nodeId, v, s, node.sk);
                    if (e.commits.add(node.nodeId)) {
                        e.commitSignedMsgs.put(node.nodeId, cEnv);
                    }
                } catch (Exception ex) {
                    log.error("Failed to encode self Commit v={} s={} on {}: {}", v, s, node.nodeId, ex.getMessage());
                }
            }
        });
        log.debug("ACCEPT Prepare v={} s={} signer={} on {}", v, s, tr.signerId(), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void sendCommit(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodeCommit(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT Commit v={} s={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        if (!tr.viewMatches() || !tr.seqMatches()) {
            log.warn("REJECT Commit v={} s={} signer={} reason=signed-message/payload mismatch on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view/seq mismatch").asRuntimeException());
            return;
        }
        if (!ctx.peers.isLive(tr.signerId())) {
            log.warn("DROP Commit from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }
        long v = tr.message().getView();
        long s = tr.message().getSeq();
        if (ctx.state.viewChangeActive && v == ctx.state.currentView()) {
            ctx.state.viewChangeActive = false;
            ctx.state.pendingNewView = 0;
            ctx.timers.onViewStart(v);
        }
        long currentView = ctx.state.currentView();
        if (v != currentView) {
            log.warn("REJECT Commit v={} s={} signer={} reason=unexpected view current={} on {}", v, s, tr.signerId(), currentView, node.nodeId);
            resp.onError(Status.FAILED_PRECONDITION.withDescription("unexpected view").asRuntimeException());
            return;
        }
        String primaryId2 = ViewUtil.primaryId(v, node.n);
        if (!node.nodeId.equals(primaryId2)) {
            log.warn("REJECT Commit v={} s={} signer={} reason=role violation (receiver not primary, primary={}) on {}", v, s, tr.signerId(), primaryId2, node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("role violation: only primary may receive Commit").asRuntimeException());
            return;
        }
        if (!tr.signerId().equals(tr.message().getReplicaId())) {
            log.warn("REJECT Commit v={} s={} signer={} reason=replicaId mismatch inner={} on {}", v, s, tr.signerId(), tr.message().getReplicaId(), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("replicaId/signature mismatch").asRuntimeException());
            return;
        }
        byte[] digestBytes = tr.message().getRequestDigest().toByteArray();
        var existing = ctx.state.get(v, s);
        if (existing != null && existing.requestDigest != null && !Arrays.equals(existing.requestDigest, digestBytes)) {
            log.warn("REJECT Commit v={} s={} signer={} reason=digest mismatch existing={} new={} on {}",
                    v, s, tr.signerId(), Hex.toHexOrNullLiteral(existing.requestDigest), Hex.toHexOrNullLiteral(digestBytes), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("commit digest mismatch").asRuntimeException());
            return;
        }
        try { ctx.state.recordEvent("IN", "pbft.Commit", v, s, tr.signerId(), node.nodeId, "", 0L, tr.message().getRequestDigest().toByteArray(), "OK", ""); } catch (Exception ignored) {}
        engine.submit("Commit (" + v + "," + s + ")", () -> {
            var e = ctx.state.getOrCreate(v, s);
            if (!ReplicaState.setOrVerifyDigest(e, digestBytes)) {
                log.warn("IGNORE Commit v={} s={} signer={} due to digest conflict on {}", v, s, tr.signerId(), node.nodeId);
                return;
            }
            if (e.commits.add(tr.signerId())) {
                e.commitSignedMsgs.put(tr.signerId(), signedMsg);
            }
            int threshold = node.n - node.f;
            if (e.commits.size() >= threshold && !e.commitProofSent) {
                if (ctx.sender.isByzantine() && (ctx.sender.hasAttackType("crash") || ctx.sender.hasAttackType("failstop"))) {
                    String mode = ctx.sender.hasAttackType("failstop") ? "fail-stop" : "crash";
                    log.info("C  v={} s={} primary {}: suppress CommitProof and commit on {}", v, s, mode, node.nodeId);
                    return;
                }
                CommitProof cp = CommitProof.newBuilder()
                        .setView(v)
                        .setSeq(s)
                        .setRequestDigest(com.google.protobuf.ByteString.copyFrom(e.requestDigest))
                        .addAllSignedCommits(e.commitSignedMsgs.values())
                        .build();
                e.commitProofSent = true;
                ctx.outbound.execute(() -> {
                    var res = ctx.sender.broadcastCommitProof(cp, RPC_DEADLINE_MS);
                    logSendResults("CommitProof", v, s, res);
                    try { recordOutSendResults("CommitProof", v, s, res); } catch (Exception ignored) {}
                });
                e.committedFlag = true;
                e.prepared = true;
                e.committed.complete(null);
                ctx.timers.pulse("primary-commit-quorum");
                log.info("C  v={} s={} committed (primary) on {}", v, s, node.nodeId);
                executeReady();
            }
        });
        log.debug("ACCEPT Commit v={} s={} signer={} on {}", v, s, tr.signerId(), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void sendPrepareProof(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodePrepareProof(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT PrepareProof v={} s={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        if (!tr.viewMatches() || !tr.seqMatches()) {
            log.warn("REJECT PrepareProof v={} s={} signer={} reason=signed-message/payload mismatch on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view/seq mismatch").asRuntimeException());
            return;
        }
        if (!ctx.peers.isLive(tr.signerId())) {
            log.warn("DROP PrepareProof from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }
        long v = tr.message().getView();
        long s = tr.message().getSeq();
        if (ctx.state.viewChangeActive && v == ctx.state.currentView()) {
            ctx.state.viewChangeActive = false;
            ctx.state.pendingNewView = 0;
            ctx.timers.onViewStart(v);
        }
        boolean roleOk = tr.signerId().equals(ViewUtil.primaryId(v, node.n));
        if (!roleOk) {
            log.warn("REJECT PrepareProof v={} s={} signer={} reason=role violation (primary={}) on {}", v, s, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("role violation: only primary may send PrepareProof").asRuntimeException());
            return;
        }
        long currentView = ctx.state.currentView();
        if (v != currentView) {
            log.warn("REJECT PrepareProof v={} s={} signer={} reason=unexpected view current={} on {}", v, s, tr.signerId(), currentView, node.nodeId);
            resp.onError(Status.FAILED_PRECONDITION.withDescription("unexpected view").asRuntimeException());
            return;
        }
        if (ctx.sender.isByzantine() && (ctx.sender.hasAttackType("crash") || ctx.sender.hasAttackType("failstop")) && !node.nodeId.equals(ViewUtil.primaryId(v, node.n))) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        var pv = ProofValidator.validatePrepareProof(tr.message(), node.keyStore, node.n, node.f);
        if (!pv.ok()) {
            log.warn("REJECT PrepareProof v={} s={} signer={} reason=bad proof: {} on {}", v, s, tr.signerId(), pv.reason(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("invalid PrepareProof: " + pv.reason()).asRuntimeException());
            return;
        }
        try { ctx.state.recordEvent("IN", "pbft.PrepareProof", v, s, tr.signerId(), node.nodeId, "", 0L, tr.message().getRequestDigest().toByteArray(), "OK", ""); } catch (Exception ignored) {}
        engine.submit("PrepareProof (" + v + "," + s + ")", () -> {
            var e = ctx.state.getOrCreate(v, s);
            byte[] digestBytes = tr.message().getRequestDigest().toByteArray();
            if (!ReplicaState.setOrVerifyDigest(e, digestBytes)) {
                log.warn("IGNORE PrepareProof v={} s={} on {} due to digest conflict", v, s, node.nodeId);
                return;
            }
            e.prepared = true;
            e.prePrepared = true;
            e.prepareProof = tr.message();
            ctx.timers.pulse("backup-prepare-proof-accepted");
            try {
                ctx.timers.onPrePrepareAccepted(v, s);
            } catch (Exception ignored) {
            }
            if (e.clientRequest == null && e.requestDigest != null && !e.requestFetchInProgress) {
                e.requestFetchInProgress = true;
                try {
                    requestFetchFromPeers(v, s, e.requestDigest, "", 0L);
                } catch (Exception ignored) { e.requestFetchInProgress = false; }
            }
            log.info("P  v={} s={} prepared (backup) on {}", v, s, node.nodeId);
            Commit c = Commit.newBuilder()
                    .setView(v).setSeq(s)
                    .setRequestDigest(tr.message().getRequestDigest())
                    .setReplicaId(node.nodeId)
                    .build();
            try {
                SignedMessage cSignedMsg = SignedMessagePacker.encode(RequestType.COMMIT, PbftMsgTypes.COMMIT, c, node.nodeId, v, s, node.sk);
                if (e.commits.add(node.nodeId)) {
                    e.commitSignedMsgs.put(node.nodeId, cSignedMsg);
                }
            } catch (Exception ex) {
                log.error("Failed to encode Commit from backup v={} s={} on {}: {}", v, s, node.nodeId, ex.getMessage());
            }
            ctx.outbound.execute(() -> {
                String status = ctx.sender.sendCommitToPrimary(c, RPC_DEADLINE_MS);
                if (!"OK".equals(status)) {
                    log.warn("SEND Commit to primary v={} s={} from={} status={} on {}", v, s, node.nodeId, status, node.nodeId);
                }
                try { ctx.state.recordEvent("OUT", "pbft.Commit", v, s, node.nodeId, ViewUtil.primaryId(v, node.n), "", 0L, c.getRequestDigest().toByteArray(), status, ""); } catch (Exception ignored) {}
            });
        });
        log.debug("ACCEPT PrepareProof v={} s={} signer={} primary={} on {}", v, s, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void sendCommitProof(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodeCommitProof(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT CommitProof v={} s={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        if (!tr.viewMatches() || !tr.seqMatches()) {
            log.warn("REJECT CommitProof v={} s={} signer={} reason=signed-message/payload mismatch on {}", signedMsg.getView(), signedMsg.getSeq(), signedMsg.getSignerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view/seq mismatch").asRuntimeException());
            return;
        }
        if (!ctx.peers.isLive(tr.signerId())) {
            log.warn("DROP CommitProof from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }
        long v = tr.message().getView();
        long s = tr.message().getSeq();
        if (ctx.state.viewChangeActive && v == ctx.state.currentView()) {
            ctx.state.viewChangeActive = false;
            ctx.state.pendingNewView = 0;
            ctx.timers.onViewStart(v);
        }
        boolean roleOk = tr.signerId().equals(ViewUtil.primaryId(v, node.n));
        if (!roleOk) {
            log.warn("REJECT CommitProof v={} s={} signer={} reason=role violation (primary={}) on {}", v, s, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("role violation: only primary may send CommitProof").asRuntimeException());
            return;
        }
        long currentView = ctx.state.currentView();
        if (v != currentView) {
            log.warn("REJECT CommitProof v={} s={} signer={} reason=unexpected view current={} on {}", v, s, tr.signerId(), currentView, node.nodeId);
            resp.onError(Status.FAILED_PRECONDITION.withDescription("unexpected view").asRuntimeException());
            return;
        }
        if (ctx.sender.isByzantine() && ctx.sender.hasAttackType("crash") && !node.nodeId.equals(ViewUtil.primaryId(v, node.n))) {
            resp.onNext(Empty.getDefaultInstance());
            resp.onCompleted();
            return;
        }
        var cv = ProofValidator.validateCommitProof(tr.message(), node.keyStore, node.n, node.f);
        if (!cv.ok()) {
            log.warn("REJECT CommitProof v={} s={} signer={} reason=bad proof: {} on {}", v, s, tr.signerId(), cv.reason(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("invalid CommitProof: " + cv.reason()).asRuntimeException());
            return;
        }
        try { ctx.state.recordEvent("IN", "pbft.CommitProof", v, s, tr.signerId(), node.nodeId, "", 0L, tr.message().getRequestDigest().toByteArray(), "OK", ""); } catch (Exception ignored) {}
        engine.submit("CommitProof (" + v + "," + s + ")", () -> {
            var e = ctx.state.getOrCreate(v, s);
            byte[] digestBytes = tr.message().getRequestDigest().toByteArray();
            if (!ReplicaState.setOrVerifyDigest(e, digestBytes)) {
                log.warn("IGNORE CommitProof v={} s={} on {} due to digest conflict", v, s, node.nodeId);
                return;
            }
            e.commitProofSent = true;
            e.prePrepared = true;
            e.prepared = true;
            e.committedFlag = true;
            e.committed.complete(null);
            ctx.timers.pulse("commit-proof-accepted");
            try {
                ctx.timers.onPrePrepareAccepted(v, s);
            } catch (Exception ignored) {
            }
            if (s <= ctx.state.lastStableSeq) {
                e.executed = true;
            }
            if (e.clientRequest == null && e.requestDigest != null && !e.requestFetchInProgress) {
                e.requestFetchInProgress = true;
                try {
                    requestFetchFromPeers(v, s, e.requestDigest, "", 0L);
                } catch (Exception ignored) { e.requestFetchInProgress = false; }
            }
            log.info("C  v={} s={} committed on {}", v, s, node.nodeId);
            executeReady();
        });
        log.debug("ACCEPT CommitProof v={} s={} signer={} primary={} on {}", v, s, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void sendViewChange(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodeViewChange(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT ViewChange v={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        if (!tr.viewMatches()) {
            log.warn("REJECT ViewChange v={} signer={} reason=signed-message/payload mismatch on {}", signedMsg.getView(), signedMsg.getSignerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view mismatch").asRuntimeException());
            return;
        }
        if (!tr.signerId().equals(tr.message().getReplicaId())) {
            log.warn("REJECT ViewChange v={} signer={} reason=replicaId/signature mismatch inner={} on {}", signedMsg.getView(), tr.signerId(), tr.message().getReplicaId(), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("replicaId/signature mismatch").asRuntimeException());
            return;
        }
        if (!ctx.peers.isLive(tr.signerId())) {
            log.warn("DROP ViewChange from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }
        long vPrime = tr.message().getNewView();
        engine.submit("ViewChange (v'=" + vPrime + ")", () -> {
            if (vPrime <= ctx.state.currentView()) {
                log.debug("IGNORE ViewChange v'={} at current={} on {}", vPrime, ctx.state.currentView(), node.nodeId);
                return;
            }
            ctx.state.recordViewChange(vPrime, tr.signerId(), signedMsg);
            ctx.timers.onViewChangeReceived(vPrime, tr.signerId());
            if (node.nodeId.equals(ViewUtil.primaryId(vPrime, node.n)) && !ctx.state.hasSentNewView(vPrime)) {
                var vcMap = ctx.state.viewChangesFor(vPrime);
                if (vcMap.size() >= (2 * node.f + 1)) {
                    buildAndBroadcastNewView(vPrime, new ArrayList<>(vcMap.values()));
                }
            }
        });
        try { ctx.state.recordEvent("IN", "pbft.ViewChange", vPrime, 0, tr.signerId(), node.nodeId, "", 0L, null, "OK", ""); } catch (Exception ignored) {}
        log.debug("ACCEPT ViewChange v'={} signer={} on {}", vPrime, tr.signerId(), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    @Override
    public void sendNewView(SignedMessage signedMsg, StreamObserver<Empty> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        var tr = MessageDecoder.decodeNewView(signedMsg, node.keyStore);
        if (!tr.ok()) {
            log.warn("REJECT NewView v'={} signer={} reason={} on {}", signedMsg.getView(), signedMsg.getSignerId(), tr.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException());
            return;
        }
        long v = tr.message().getNewView();
        if (!tr.viewMatches()) {
            log.warn("REJECT NewView v'={} signer={} reason=signed-message/payload mismatch on {}", v, tr.signerId(), node.nodeId);
            resp.onError(Status.INVALID_ARGUMENT.withDescription("signed message header view mismatch").asRuntimeException());
            return;
        }
        if (!ctx.peers.isLive(tr.signerId())) {
            log.warn("DROP NewView from non-live sender={} on {}", tr.signerId(), node.nodeId);
            resp.onError(Status.UNAVAILABLE.withDescription("peer not live").asRuntimeException());
            return;
        }
        boolean roleOk = tr.signerId().equals(ViewUtil.primaryId(v, node.n));
        if (!roleOk) {
            log.warn("REJECT NewView v'={} signer={} reason=role violation (primary={}) on {}", v, tr.signerId(), ViewUtil.primaryId(v, node.n), node.nodeId);
            resp.onError(Status.PERMISSION_DENIED.withDescription("role violation: only primary may send NewView").asRuntimeException());
            return;
        }
        try { ctx.state.recordEvent("IN", "pbft.NewView", v, 0L, tr.signerId(), node.nodeId, "", 0L, null, "OK", ""); } catch (Exception ignored) {}
        engine.submit("AcceptNewView (v'=" + v + ")", () -> {
            if (v <= ctx.state.currentView()) {
                log.debug("IGNORE NewView v'={} at current={} on {}", v, ctx.state.currentView(), node.nodeId);
                return;
            }
            if (ctx.state.viewChangeActive && ctx.state.pendingNewView > 0 && v < ctx.state.pendingNewView) {
                log.debug("IGNORE NewView v'={} below pending {} on {}", v, ctx.state.pendingNewView, node.nodeId);
                return;
            }
            if (!validateNewView(tr.message())) {
                log.warn("REJECT NewView v'={} reason=validation failed on {}", v, node.nodeId);
                return;
            }
            acceptNewView(tr.message());
        });
        log.debug("ACCEPT NewView v'={} signer={} on {}", v, tr.signerId(), node.nodeId);
        resp.onNext(Empty.getDefaultInstance());
        resp.onCompleted();
    }

    private void logSendResults(String kind, long view, long seq, Map<String, String> results) {
        if (results == null || results.isEmpty()) return;
        boolean detailed = Boolean.getBoolean("pbft.log.sendDetails");
        if (detailed) {
            for (var e : results.entrySet()) {
                if (!"OK".equals(e.getValue())) {
                    log.warn("SEND {} v={} s={} target={} status={} on {}", kind, view, seq, e.getKey(), e.getValue(), node.nodeId);
                }
            }
            return;
        }
        int ok = 0;
        java.util.Map<String, Integer> errs = new java.util.HashMap<>();
        java.util.List<String> sample = new java.util.ArrayList<>();
        for (var e : results.entrySet()) {
            if ("OK".equals(e.getValue())) {
                ok++;
                continue;
            }
            errs.merge(e.getValue(), 1, Integer::sum);
            if (sample.size() < 3) sample.add(e.getKey() + ":" + e.getValue());
        }
        if (!errs.isEmpty()) {
            log.warn("SEND {} v={} s={} ok={} errs={} sample={} on {}", kind, view, seq, ok, errs, sample, node.nodeId);
        }
    }

    private void recordOutSendResults(String kind, long view, long seq, java.util.Map<String, String> results) {
        if (results == null || results.isEmpty()) return;
        String type = "pbft." + kind;
        for (var e : results.entrySet()) {
            try { ctx.state.recordEvent("OUT", type, view, seq, node.nodeId, e.getKey(), "", 0L, null, e.getValue(), ""); } catch (Exception ignored) {}
        }
    }

    @Override
    public void getClientRequest(RequestDigest req, StreamObserver<SignedMessage> resp) {
        if (!ctx.liveSelf) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException());
            return;
        }
        engine.submit("GetClientRequest", () -> {
            SignedMessage found = null;
            if (!req.getClientId().isEmpty()) {
                var key = ctx.state.findByClientReq(req.getClientId(), req.getReqSeq());
                if (key != null) {
                    var e = ctx.state.get(key.view, key.seq);
                    if (e != null) {
                        if (e.clientRequestSignedMsg != null) {
                            found = e.clientRequestSignedMsg;
                        } else if (e.prePrepare != null && e.prePrepare.hasClientRequest()) {
                            found = e.prePrepare.getClientRequest();
                        }
                    }
                }
            }
            if (found == null && !req.getRequestDigest().isEmpty()) {
                byte[] target = req.getRequestDigest().toByteArray();
                for (var entry : ctx.state.snapshotLog().entrySet()) {
                    var en = entry.getValue();
                    if (en.requestDigest != null && java.util.Arrays.equals(en.requestDigest, target)) {
                        if (en.clientRequestSignedMsg != null) {
                            found = en.clientRequestSignedMsg;
                            break;
                        }
                        if (en.prePrepare != null && en.prePrepare.hasClientRequest()) {
                            found = en.prePrepare.getClientRequest();
                            break;
                        }
                    }
                }
            }
            if (found != null) {
                resp.onNext(found);
                resp.onCompleted();
            } else {
                resp.onError(Status.NOT_FOUND.withDescription("request not available").asRuntimeException());
            }
        });
    }

    private void requestFetchFromPeers(long view, long seq, byte[] digest, String clientId, long reqSeq) {
        RequestDigest.Builder b = RequestDigest.newBuilder().setRequestDigest(com.google.protobuf.ByteString.copyFrom(digest));
        if (clientId != null && !clientId.isEmpty()) b.setClientId(clientId);
        if (reqSeq != 0) b.setReqSeq(reqSeq);
        RequestDigest rd = b.build();
        ctx.outbound.execute(() -> {
            for (var e : ctx.peers.replicaStubs().entrySet()) {
                try {
                    var stub = e.getValue().withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                    SignedMessage sm = stub.getClientRequest(rd);
                    var tr = MessageDecoder.decodeClientRequest(sm, node.keyStore);
                    if (!tr.ok()) continue;
                    byte[] calc = Digests.sha256(tr.message().toByteArray());
                    if (!java.util.Arrays.equals(calc, digest)) continue;
                    engine.submit("InstallFetchedRequest (" + view + "," + seq + ")", () -> {
                        var en = ctx.state.get(view, seq);
                        if (en != null && en.requestDigest != null && java.util.Arrays.equals(en.requestDigest, digest)) {
                            en.clientRequest = tr.message();
                            en.clientRequestSignedMsg = sm;
                            en.requestFetchInProgress = false;
                            if (!tr.message().getClientId().isEmpty() && tr.message().getReqSeq() != 0L) {
                                ctx.state.indexClientReq(tr.message().getClientId(), tr.message().getReqSeq(), new ReplicaState.EntryKey(view, seq));
                            }
                            if (en.committedFlag && !en.executed) {
                                try {
                                    executeReady();
                                } catch (Exception ignored) {
                                }
                            }
                        }
                    });
                    return;
                } catch (io.grpc.StatusRuntimeException ex) {
                } catch (Exception ex) {
                }
            }
            engine.submit("InstallFetchedRequest (not found)", () -> {
                var en = ctx.state.get(view, seq);
                if (en != null) {
                    en.requestFetchInProgress = false;
                    if (en.committedFlag && !en.executed && en.requestDigest != null) {
                        long retryMs = Math.max(100L, Long.getLong("pbft.request_fetch.retry_ms", 300L));
                        ctx.outbound.execute(() -> {
                            try { Thread.sleep(retryMs); } catch (InterruptedException ignored) {}
                            engine.submit("RetryFetch(" + view + "," + seq + ")", () -> {
                                var entry = ctx.state.get(view, seq);
                                if (entry != null && entry.committedFlag && !entry.executed && entry.requestDigest != null && !entry.requestFetchInProgress) {
                                    entry.requestFetchInProgress = true;
                                    String cid = (entry.prePrepare != null) ? entry.prePrepare.getClientId() : clientId;
                                    long t = (entry.prePrepare != null) ? entry.prePrepare.getReqSeq() : reqSeq;
                                    requestFetchFromPeers(view, seq, entry.requestDigest, cid == null ? "" : cid, t);
                                }
                            });
                        });
                    }
                }
            });
        });
    }
    private void executeReady() {
        long next = ctx.state.nextExecSeq();
        while (true) {
            long currentView = ctx.state.currentView();
            var e = ctx.state.get(currentView, next);
            if (e == null || !e.committedFlag || e.executed) break;
            String dedupClientId = null;
            long dedupReqSeq = 0L;
            if (e.clientRequest != null) {
                dedupClientId = e.clientRequest.getClientId();
                dedupReqSeq = e.clientRequest.getReqSeq();
            } else if (e.prePrepare != null) {
                dedupClientId = e.prePrepare.getClientId();
                dedupReqSeq = e.prePrepare.getReqSeq();
            }
            if (dedupClientId != null && !dedupClientId.isEmpty() && dedupReqSeq != 0L) {
                if (ctx.state.hasExecuted(dedupClientId, dedupReqSeq)) {
                    SignedMessage cachedEnv = ctx.state.getExecutedReplyEnvelope(dedupClientId, dedupReqSeq);
                    ClientReply cached = ctx.state.getExecutedReply(dedupClientId, dedupReqSeq);
                    try {
                        if (cachedEnv != null) {
                            ctx.clientReplies.deliver(dedupClientId, cachedEnv, CLIENT_REPLY_DEADLINE_MS);
                            e.finalReplySignedMsg = cachedEnv;
                            e.finalReply = cached;
                        } else if (cached != null) {
                            SignedMessage reEnv = SignedMessagePacker.encode(
                                    RequestType.CLIENT_REPLY,
                                    PbftMsgTypes.CLIENT_REPLY,
                                    cached,
                                    ctx.node.nodeId,
                                    cached.getView(),
                                    next,
                                    ctx.node.sk);
                            e.finalReplySignedMsg = reEnv;
                            e.finalReply = cached;
                            ctx.clientReplies.deliver(dedupClientId, reEnv, CLIENT_REPLY_DEADLINE_MS);
                        }
                    } catch (GeneralSecurityException ex) {
                        log.error("Failed to (re)sign cached ClientReply for v={} s={} on {}: {}", currentView, next, ctx.node.nodeId, ex.getMessage());
                    }
                    e.executed = true;
                    ctx.timers.pulse("executed");
                    ctx.timers.onExecuted();
                    ctx.state.advanceExecSeq();
                    next = ctx.state.nextExecSeq();
                    continue;
                }
            }
            if (e.clientRequest == null && e.prePrepare != null && e.prePrepare.hasClientRequest()) {
                var crTr = MessageDecoder.decodeClientRequest(e.prePrepare.getClientRequest(), ctx.node.keyStore);
                if (crTr.ok()) {
                    e.clientRequest = crTr.message();
                    e.clientRequestSignedMsg = e.prePrepare.getClientRequest();
                }
            }
            if (e.clientRequest == null) {
                if (e.requestDigest != null && java.util.Arrays.equals(e.requestDigest, NULL_DIGEST)) {
                    e.executed = true;
                    ctx.timers.pulse("executed");
                    ctx.timers.onExecuted();
                    ctx.state.advanceExecSeq();
                    next = ctx.state.nextExecSeq();
                    continue;
                }
                if (!e.requestFetchInProgress && e.requestDigest != null) {
                    e.requestFetchInProgress = true;
                    String cid = (e.prePrepare != null) ? e.prePrepare.getClientId() : "";
                    long t = (e.prePrepare != null) ? e.prePrepare.getReqSeq() : 0L;
                    requestFetchFromPeers(currentView, next, e.requestDigest, cid, t);
                }
                log.debug("DEFERRING execution of v={} s={} until client request is available on {}", ctx.state.currentView(), next, ctx.node.nodeId);
                break;
            }
            long seqNo = next;
            ClientReply.Builder replyB = ClientReply.newBuilder()
                    .setClientId(e.clientRequest.getClientId())
                    .setReqSeq(e.clientRequest.getReqSeq())
                    .setView(currentView)
                    .setReplicaId(ctx.node.nodeId);
            if (e.clientRequest.getOperation().hasTransfer()) {
                var t = e.clientRequest.getOperation().getTransfer();
                long amt = t.getAmount();
                if (amt < 0) {
                    replyB.setStatus("Invalid amount");
                } else {
                    long fromBalCur = ctx.db.getBalance(t.getFrom());
                    if (fromBalCur < amt) {
                        replyB.setStatus("Insufficient balance");
                    } else {
                        boolean ok = ctx.db.transfer(t.getFrom(), t.getTo(), amt);
                        replyB.setStatus(ok ? "OK" : "ERR");
                    }
                }
            }
            if (e.clientRequest.getOperation().hasDepositChecking()) {
                var op = e.clientRequest.getOperation().getDepositChecking();
                long amt = op.getAmount();
                if (amt < 0) {
                    replyB.setStatus("Invalid amount");
                } else if (!Boolean.getBoolean("pbft.benchmark.smallbank.enabled")) {
                    replyB.setStatus("ERR");
                } else {
                    boolean ok = ctx.db.depositChecking(op.getAccount(), amt);
                    replyB.setStatus(ok ? "OK" : "ERR");
                }
            }
            if (e.clientRequest.getOperation().hasTransactSavings()) {
                var op = e.clientRequest.getOperation().getTransactSavings();
                long amt = op.getAmount();
                if (!Boolean.getBoolean("pbft.benchmark.smallbank.enabled")) {
                    replyB.setStatus("ERR");
                } else {
                    boolean ok = ctx.db.transactSavings(op.getAccount(), amt);
                    replyB.setStatus(ok ? "OK" : "Insufficient balance");
                }
            }
            if (e.clientRequest.getOperation().hasWriteCheck()) {
                var op = e.clientRequest.getOperation().getWriteCheck();
                long amt = op.getAmount();
                if (amt < 0) {
                    replyB.setStatus("Invalid amount");
                } else if (!Boolean.getBoolean("pbft.benchmark.smallbank.enabled")) {
                    replyB.setStatus("ERR");
                } else {
                    boolean ok = ctx.db.writeCheck(op.getAccount(), amt);
                    replyB.setStatus(ok ? "OK" : "ERR");
                }
            }
            if (e.clientRequest.getOperation().hasSendPayment()) {
                var op = e.clientRequest.getOperation().getSendPayment();
                long amt = op.getAmount();
                if (amt < 0) {
                    replyB.setStatus("Invalid amount");
                } else if (!Boolean.getBoolean("pbft.benchmark.smallbank.enabled")) {
                    replyB.setStatus("ERR");
                } else {
                    boolean ok = ctx.db.sendPayment(op.getSrc(), op.getDst(), amt);
                    replyB.setStatus(ok ? "OK" : "Insufficient balance");
                }
            }
            if (e.clientRequest.getOperation().hasAmalgamate()) {
                var op = e.clientRequest.getOperation().getAmalgamate();
                if (!Boolean.getBoolean("pbft.benchmark.smallbank.enabled")) {
                    replyB.setStatus("ERR");
                } else {
                    boolean ok = ctx.db.amalgamate(op.getSrc(), op.getDst());
                    replyB.setStatus(ok ? "OK" : "ERR");
                }
            }
            if (e.clientRequest.getOperation().hasBalance()) {
                var b = e.clientRequest.getOperation().getBalance();
                long bal = Boolean.getBoolean("pbft.benchmark.smallbank.enabled")
                        ? ctx.db.getTotal(b.getAccount())
                        : ctx.db.getBalance(b.getAccount());
                replyB.setBalance(bal);
            }
            ClientReply reply = replyB.build();
            e.finalReply = reply;
            try {
                SignedMessage replyEnv = SignedMessagePacker.encode(
                        RequestType.CLIENT_REPLY,
                        PbftMsgTypes.CLIENT_REPLY,
                        reply,
                        ctx.node.nodeId,
                        currentView,
                        seqNo,
                        ctx.node.sk);
                if (ctx.sender.isByzantine() && ctx.sender.hasAttackType("sign")) {
                    replyEnv = replyEnv.toBuilder().setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0})).build();
                }
                e.finalReplySignedMsg = replyEnv;
                e.executed = true;
                if (reply.hasStatus()) {
                    long tLogical = e.clientRequest.getReqSeq();
                    var op = e.clientRequest.getOperation();
                    String desc = "UNKNOWN";
                    if (op.hasTransfer()) {
                        var t = op.getTransfer();
                        desc = "Transfer(" + t.getFrom() + "->" + t.getTo() + ", amt=" + t.getAmount() + ")";
                    } else if (op.hasDepositChecking()) {
                        var o = op.getDepositChecking();
                        desc = "DepositChecking(" + o.getAccount() + ", amt=" + o.getAmount() + ")";
                    } else if (op.hasTransactSavings()) {
                        var o = op.getTransactSavings();
                        desc = "TransactSavings(" + o.getAccount() + ", amt=" + o.getAmount() + ")";
                    } else if (op.hasWriteCheck()) {
                        var o = op.getWriteCheck();
                        desc = "WriteCheck(" + o.getAccount() + ", amt=" + o.getAmount() + ")";
                    } else if (op.hasSendPayment()) {
                        var o = op.getSendPayment();
                        desc = "SendPayment(" + o.getSrc() + "->" + o.getDst() + ", amt=" + o.getAmount() + ")";
                    } else if (op.hasAmalgamate()) {
                        var o = op.getAmalgamate();
                        desc = "Amalgamate(" + o.getSrc() + "->" + o.getDst() + ")";
                    }
                    log.info("E  v={} s={} {} = {} t={} on {}", currentView, seqNo, desc, reply.getStatus(), tLogical, node.nodeId);
                    try { ctx.state.recordEvent("OUT", "pbft.ClientReply", currentView, seqNo, node.nodeId, e.clientRequest.getClientId(), e.clientRequest.getClientId(), e.clientRequest.getReqSeq(), e.requestDigest, reply.getStatus(), desc); } catch (Exception ignored) {}
                } else if (reply.hasBalance()) {
                    var b = e.clientRequest.getOperation().getBalance();
                    long tLogical = e.clientRequest.getReqSeq();
                    log.info("E  v={} s={} Balance({}) = {} t={} on {}", currentView, seqNo, b.getAccount(), reply.getBalance(), tLogical, node.nodeId);
                    try { ctx.state.recordEvent("OUT", "pbft.ClientReply", currentView, seqNo, node.nodeId, e.clientRequest.getClientId(), e.clientRequest.getClientId(), e.clientRequest.getReqSeq(), e.requestDigest, Long.toString(reply.getBalance()), "Balance(" + b.getAccount() + ")"); } catch (Exception ignored) {}
                }
                ctx.timers.pulse("executed");
                ctx.timers.onExecuted();
                ctx.state.recordExecutedReply(e.clientRequest.getClientId(), e.clientRequest.getReqSeq(), reply, replyEnv);
                ctx.clientReplies.deliver(e.clientRequest.getClientId(), replyEnv, CLIENT_REPLY_DEADLINE_MS);
            } catch (GeneralSecurityException ex) {
                log.error("Failed to sign ClientReply for v={} s={} on {}: {}", currentView, seqNo, ctx.node.nodeId, ex.getMessage());
                e.executed = true;
            }
            ctx.state.advanceExecSeq();
            if (Boolean.getBoolean("pbft.checkpoints.enabled")) {
                long period = Math.max(1L, Long.getLong("pbft.checkpoints.period", 100L));
                if (seqNo % period == 0) {
                    try {
                        byte[] payload = ctx.db.canonicalSnapshotBytes();
                        byte[] dg = Digests.sha256(payload);
                        Checkpoint cp = Checkpoint.newBuilder()
                                .setSeq(seqNo)
                                .setStateDigest(ByteString.copyFrom(dg))
                                .setReplicaId(node.nodeId)
                                .build();
                        SignedMessage env = SignedMessagePacker.encode(RequestType.CHECKPOINT, PbftMsgTypes.CHECKPOINT, cp, node.nodeId, 0, seqNo, node.sk);
                        ctx.state.recordLocalCheckpoint(seqNo, dg, env, payload);
                        ctx.outbound.execute(() -> {
                    var res = ctx.sender.broadcastCheckpoint(cp, RPC_DEADLINE_MS);
                    logSendResults("Checkpoint", 0, seqNo, res);
                    try { recordOutSendResults("Checkpoint", 0, seqNo, res); } catch (Exception ignored) {}
                });
                        int signers = ctx.state.uniqueCheckpointSignerCount(seqNo, dg);
                        if (signers >= (2 * node.f + 1)) {
                            ctx.state.setStableCheckpoint(seqNo, dg, ctx.state.checkpointEnvelopes(seqNo, dg));
                            log.info("CHKPT s={} stable ({} signers) on {}", seqNo, signers, node.nodeId);
                        }
                    } catch (Exception ignore) {
                    }
                }
            }
            next = ctx.state.nextExecSeq();
            continue;
        }
    }
}
