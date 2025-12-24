package pbft.replica.rpc;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pbft.common.validation.*;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import pbft.proto.*;
import pbft.replica.rpc.RpcSupport.NodeInfo;
import pbft.replica.core.ReplicaContext;
import pbft.replica.core.ReplicaState;
import pbft.replica.core.ConsensusEngine;
import pbft.common.crypto.Digests;
import pbft.common.util.Hex;
import pbft.common.crypto.RequestType;

import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.Arrays;
import java.util.Map;


public final class ClientRpcService extends ClientServiceGrpc.ClientServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(ClientRpcService.class);
    private final NodeInfo node;
    private final ReplicaContext replicaContext;
    private final ConsensusEngine engine;
    private static final long RPC_DEADLINE_MS = Math.max(4000L, Long.getLong("pbft.rpc.deadline_ms", 8000L));

    public ClientRpcService(NodeInfo node, ConsensusEngine engine, ReplicaContext replicaContext) {
        this.node = node; this.engine = engine; this.replicaContext = replicaContext;
    }

    @Override
    public void submit(SignedMessage signedMessage, StreamObserver<SignedMessage> resp) {
        if (replicaContext.sender.isByzantine() &&
                (replicaContext.sender.hasAttackType("failstop") || replicaContext.sender.hasAttackType("crash"))) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica unavailable").asRuntimeException());
            return;
        }
        if (!replicaContext.liveSelf) { resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException()); return; }
        var result = MessageDecoder.decodeClientRequest(signedMessage, node.keyStore);
        try {
            if (result.ok()) {
                byte[] dg0 = pbft.common.crypto.Digests.sha256(result.message().toByteArray());
                replicaContext.state.recordEvent("IN", "pbft.ClientRequest", replicaContext.state.currentView(), 0L, result.signerId(), node.nodeId, result.message().getClientId(), result.message().getReqSeq(), dg0, "OK", "submit");
            } else {
                replicaContext.state.recordEvent("IN", "pbft.ClientRequest", replicaContext.state.currentView(), 0L, signedMessage.getSignerId(), node.nodeId, "", 0L, null, result.validation().code().name(), "submit");
            }
        } catch (Exception ignored) {}

        if (!result.ok()) {
            log.warn("REJECT ClientSubmit v={} s={} client={} reason={} on {}", signedMessage.getView(), signedMessage.getSeq(), signedMessage.getSignerId(), result.validation(), node.nodeId);
            resp.onError(GrpcStatusUtil.mapStatus(result.validation()).asRuntimeException());
            return;
        }
        if (replicaContext.state.viewChangeActive) {
            resp.onError(Status.UNAVAILABLE.withDescription("view change in progress").asRuntimeException());
            return;
        }
        var req = result.message();
        byte[] digest = Digests.sha256(req.toByteArray());
        long view = replicaContext.state.currentView();
        String primary = RpcSupport.ViewUtil.primaryId(view, node.n);
        if (!node.nodeId.equals(primary)) {
            SignedMessage cachedEnv = replicaContext.state.getExecutedReplyEnvelope(req.getClientId(), req.getReqSeq());
            if (cachedEnv != null) { resp.onNext(cachedEnv); resp.onCompleted(); return; }
            ClientReply cached = replicaContext.state.getExecutedReply(req.getClientId(), req.getReqSeq());
            if (cached != null) {
                try {
                    SignedMessage reEnv = SignedMessagePacker.encode(RequestType.CLIENT_REPLY, PbftMsgTypes.CLIENT_REPLY, cached, node.nodeId, cached.getView(), 0, node.sk);
                    resp.onNext(reEnv); resp.onCompleted(); return;
                } catch (GeneralSecurityException ignored) {}
            }
            try {
                var existingKey = replicaContext.state.findByClientReq(req.getClientId(), req.getReqSeq());
                if (existingKey != null) {
                    var existingEntry = replicaContext.state.get(existingKey.view, existingKey.seq);
                    if (existingEntry != null) {
                        try { existingEntry.committed.get(15000, TimeUnit.MILLISECONDS); } catch (java.util.concurrent.TimeoutException | InterruptedException ex) {}
                        SignedMessage existingReply = awaitSignedFinalReply(existingEntry, RPC_DEADLINE_MS);
                        if (existingReply != null) { resp.onNext(existingReply); resp.onCompleted(); return; }
                    }
                }
                var stub = replicaContext.peers.clientStubFor(primary).withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                SignedMessage leaderReply = stub.submit(signedMessage);
                var replyTr = MessageDecoder.decodeClientReply(leaderReply, node.keyStore);
                if (!replyTr.ok()) { resp.onError(GrpcStatusUtil.mapStatus(replyTr.validation()).asRuntimeException()); return; }
                if (!replyTr.viewMatches()) { resp.onError(Status.INVALID_ARGUMENT.withDescription("reply signed message header mismatch").asRuntimeException()); return; }
                ClientReply leaderReplyMsg = replyTr.message();
                if (!replyTr.signerId().equals(leaderReplyMsg.getReplicaId())) { resp.onError(Status.PERMISSION_DENIED.withDescription("reply signer/replica mismatch").asRuntimeException()); return; }
                if (!leaderReplyMsg.getClientId().equals(req.getClientId()) || leaderReplyMsg.getReqSeq() != req.getReqSeq()) { resp.onError(Status.INVALID_ARGUMENT.withDescription("reply does not match request").asRuntimeException()); return; }
                try { replicaContext.state.recordEvent("OUT", "pbft.ForwardToPrimary", view, 0L, node.nodeId, primary, req.getClientId(), req.getReqSeq(), pbft.common.crypto.Digests.sha256(req.toByteArray()), "OK", "submit"); } catch (Exception ignored) {}
                resp.onNext(leaderReply); resp.onCompleted();
            } catch (StatusRuntimeException e) {

                try { replicaContext.timers.onClientRequestAwaiting(view); } catch (Exception ignored) {}
                resp.onError(e.getStatus().asRuntimeException());
            } catch (Exception e) {
                try { replicaContext.timers.onClientRequestAwaiting(view); } catch (Exception ignored) {}
                resp.onError(Status.UNAVAILABLE.withDescription("forward failed: " + e.getMessage()).asRuntimeException());
            }
            return;
        }

        try {
            CompletableFuture<PrimaryInitResult> initFuture = new CompletableFuture<>();
            engine.submit("PrimaryInit (client=" + req.getClientId() + "#" + req.getReqSeq() + ")", () -> {
                var existingKey = replicaContext.state.findByClientReq(req.getClientId(), req.getReqSeq());
                if (existingKey != null) {
                    var existingEntry = replicaContext.state.get(existingKey.view, existingKey.seq);
                    if (existingEntry != null) {
                        if (existingKey.view == view) {
                            initFuture.complete(new PrimaryInitResult(existingKey, false, null));
                            return;
                        }
                        if (existingEntry.executed || existingEntry.committedFlag || existingEntry.prepared || existingEntry.finalReplySignedMsg != null) {
                            initFuture.complete(new PrimaryInitResult(existingKey, false, null));
                            return;
                        }
                        log.info("Rescheduling client={}#{} into view {} (previous v={}, s={}) on {}", req.getClientId(), req.getReqSeq(), view, existingKey.view, existingKey.seq, node.nodeId);
                        replicaContext.state.unindexClientReq(req.getClientId(), req.getReqSeq());
                        if (!existingEntry.committed.isDone()) {
                            existingEntry.committed.completeExceptionally(new CancellationException("Rescheduled to view " + view));
                        }
                    } else {
                        replicaContext.state.unindexClientReq(req.getClientId(), req.getReqSeq());
                    }
                }

                long seq = replicaContext.state.allocSeq();
                var key = new ReplicaState.EntryKey(view, seq);
                var entry = replicaContext.state.getOrCreate(view, seq);
                if (entry.requestDigest != null && !Arrays.equals(entry.requestDigest, digest)) {
                    initFuture.completeExceptionally(new IllegalStateException("conflicting digest for seq " + key));
                    return;
                }
                entry.requestDigest = Arrays.copyOf(digest, digest.length);

                String opDesc;
                if (req.getOperation().hasTransfer()) {
                    var t = req.getOperation().getTransfer();
                    opDesc = String.format("Transfer(%s->%s, amt=%d)", t.getFrom(), t.getTo(), t.getAmount());
                } else if (req.getOperation().hasBalance()) {
                    var b = req.getOperation().getBalance();
                    opDesc = String.format("Balance(%s)", b.getAccount());
                } else {
                    opDesc = "UNKNOWN";
                }
                log.info("START v={} s={} t={} op={} on {}", view, seq, req.getReqSeq(), opDesc, node.nodeId);


                boolean crash = replicaContext.sender.isByzantine() && replicaContext.sender.hasAttackType("crash");
                if (!crash) {
                    PrePrepare pp = PrePrepare.newBuilder()
                            .setView(view)
                            .setSeq(seq)
                            .setRequestDigest(com.google.protobuf.ByteString.copyFrom(digest))
                            .setClientId(req.getClientId())
                            .setReqSeq(req.getReqSeq())
                            .setClientRequest(signedMessage)
                            .build();
                    entry.prePrepare = pp;
                    entry.clientRequest = req;
                    entry.clientRequestSignedMsg = signedMessage;
                    entry.prePrepared = true;
                    String dig = Hex.toHexOrEmpty(digest);
                    if (dig.length() > 16) dig = dig.substring(0,16);
                    log.info("PP v={} s={} client={}#{} t={} dig={} on {}", view, seq, req.getClientId(), req.getReqSeq(), req.getReqSeq(), dig, node.nodeId);

                    replicaContext.timers.pulse("primary-preprepare-installed");
                    replicaContext.state.indexClientReq(req.getClientId(), req.getReqSeq(), key);
                    Prepare selfPrepare = Prepare.newBuilder()
                            .setView(view)
                            .setSeq(seq)
                            .setRequestDigest(pp.getRequestDigest())
                            .setReplicaId(node.nodeId)
                            .build();
                    try {
                        var selfSignedMsg = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, selfPrepare, node.nodeId, view, seq, node.sk);
                        entry.prepares.add(node.nodeId);
                        entry.prepareSignedMsgs.put(node.nodeId, selfSignedMsg);
                    } catch (Exception ex) {
                        initFuture.completeExceptionally(ex);
                        return;
                    }
                }

                ReplicaState.EntryKey key2 = null;
                boolean equivActive = replicaContext.sender.isByzantine() && replicaContext.sender.hasAttackType("equivocation");
                if (equivActive) {
                    long seq2 = replicaContext.state.allocSeq();
                    key2 = new ReplicaState.EntryKey(view, seq2);
                    PrePrepare pp2 = PrePrepare.newBuilder()
                            .setView(view)
                            .setSeq(seq2)
                            .setRequestDigest(com.google.protobuf.ByteString.copyFrom(digest))
                            .setClientId(req.getClientId())
                            .setReqSeq(req.getReqSeq())
                            .setClientRequest(signedMessage)
                            .build();
                    var e2 = replicaContext.state.getOrCreate(view, seq2);
                    e2.requestDigest = Arrays.copyOf(digest, digest.length);
                    e2.prePrepare = pp2;
                    e2.clientRequest = req;
                    e2.clientRequestSignedMsg = signedMessage;
                    e2.prePrepared = true;
                    try {
                        Prepare selfPr2 = Prepare.newBuilder().setView(view).setSeq(seq2).setRequestDigest(pp2.getRequestDigest()).setReplicaId(node.nodeId).build();
                        var selfSigned2 = SignedMessagePacker.encode(RequestType.PREPARE, PbftMsgTypes.PREPARE, selfPr2, node.nodeId, view, seq2, node.sk);
                        e2.prepares.add(node.nodeId);
                        e2.prepareSignedMsgs.put(node.nodeId, selfSigned2);
                    } catch (Exception ignored) {}
                    String dig2 = Hex.toHexOrEmpty(digest); if (dig2.length() > 16) dig2 = dig2.substring(0,16);
                    log.info("PP v={} s={} client={}#{} t={} dig={} on {}", view, seq2, req.getClientId(), req.getReqSeq(), req.getReqSeq(), dig2, node.nodeId);
                }

                initFuture.complete(new PrimaryInitResult(key, !crash, crash ? null : key2));
            });
            PrimaryInitResult init = initFuture.get(6, TimeUnit.SECONDS);
            var entry = replicaContext.state.get(init.key.view, init.key.seq);
            if (entry == null) {
                resp.onError(Status.INTERNAL.withDescription("entry missing after init").asRuntimeException());
                return;
            }
            if (init.broadcastNeeded) {
                boolean equiv = replicaContext.sender.isByzantine() && replicaContext.sender.hasAttackType("equivocation");
                if (equiv && init.key2 != null) {
                    java.util.Set<String> recipients = replicaContext.peers.replicaStubs().keySet();
                    java.util.Set<String> targetsA = new java.util.HashSet<>(replicaContext.sender.equivTargets());
                    targetsA.retainAll(recipients);
                    java.util.Set<String> targetsB = new java.util.HashSet<>(recipients);
                    targetsB.removeAll(targetsA);
                    if (!targetsA.isEmpty() && !targetsB.isEmpty()) {
                        replicaContext.outbound.execute(() -> {
                            var en = replicaContext.state.get(init.key.view, init.key.seq);
                            if (en == null || en.executed || en.committedFlag || en.prepared) return;
                            if (replicaContext.state.currentView() != init.key.view) return;
                            var res = replicaContext.sender.sendPrePrepareToSubset(entry.prePrepare, RPC_DEADLINE_MS, targetsA);
                            if (res != null) for (var e : res.entrySet()) { 
                                try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", init.key.view, init.key.seq, node.nodeId, e.getKey(), req.getClientId(), req.getReqSeq(), entry.prePrepare.getRequestDigest().toByteArray(), e.getValue(), "subsetA"); } catch (Exception ignored) {} 
                            }
                        });
                        final var k2 = init.key2;
                        replicaContext.outbound.execute(() -> {
                            var en2 = replicaContext.state.get(k2.view, k2.seq);
                            if (en2 == null || en2.executed || en2.committedFlag || en2.prepared) return;
                            if (replicaContext.state.currentView() != k2.view) return;
                            var res2 = replicaContext.sender.sendPrePrepareToSubset(en2.prePrepare, RPC_DEADLINE_MS, targetsB);
                            if (res2 != null) for (var e2 : res2.entrySet()) { 
                                try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", k2.view, k2.seq, node.nodeId, e2.getKey(), req.getClientId(), req.getReqSeq(), en2.prePrepare.getRequestDigest().toByteArray(), e2.getValue(), "subsetB"); } catch (Exception ignored) {} 
                            }
                        });
                        final ReplicaState.EntryKey subsetAKey = init.key;
                        final ReplicaState.EntryKey subsetBKey = init.key2;
                        scheduleSubsetPrePrepare(subsetAKey, targetsA, 300);
                        scheduleSubsetPrePrepare(subsetBKey, targetsB, 300);
                        scheduleSubsetPrePrepare(subsetAKey, targetsA, 900);
                        scheduleSubsetPrePrepare(subsetBKey, targetsB, 900);
                        scheduleSubsetPrePrepare(subsetAKey, targetsA, 1500);
                        scheduleSubsetPrePrepare(subsetBKey, targetsB, 1500);
                    } else {
                        replicaContext.outbound.execute(() -> {
                            var en = replicaContext.state.get(init.key.view, init.key.seq);
                            if (en == null || en.executed || en.committedFlag || en.prepared) return;
                            if (replicaContext.state.currentView() != init.key.view) return;
                            var res = replicaContext.sender.sendPrePrepareToAll(entry.prePrepare, RPC_DEADLINE_MS);
                            if (res != null) for (var eall : res.entrySet()) { 
                                try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", init.key.view, init.key.seq, node.nodeId, eall.getKey(), req.getClientId(), req.getReqSeq(), entry.prePrepare.getRequestDigest().toByteArray(), eall.getValue(), "all"); } catch (Exception ignored) {} 
                            }
                        });
                    }
                } else {
                    replicaContext.outbound.execute(() -> {
                        var en = replicaContext.state.get(init.key.view, init.key.seq);
                        if (en == null || en.executed || en.committedFlag || en.prepared) return;
                        if (replicaContext.state.currentView() != init.key.view) return;
                        var res = replicaContext.sender.sendPrePrepareToAll(entry.prePrepare, RPC_DEADLINE_MS);
                        if (res != null) for (var eall : res.entrySet()) { 
                            try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", init.key.view, init.key.seq, node.nodeId, eall.getKey(), req.getClientId(), req.getReqSeq(), entry.prePrepare.getRequestDigest().toByteArray(), eall.getValue(), "all"); } catch (Exception ignored) {} 
                        }
                    });
                }
                if (!equiv) {
                    replicaContext.outbound.execute(() -> {
                    try { Thread.sleep(300); } catch (InterruptedException ignored) {}
                    var en = replicaContext.state.get(init.key.view, init.key.seq);
                    if (en == null || en.executed || en.committedFlag || en.prepared) return;
                    if (replicaContext.state.currentView() != init.key.view) return;
                    var res = replicaContext.sender.sendPrePrepareToAll(entry.prePrepare, RPC_DEADLINE_MS);
                    if (res != null) for (var eall : res.entrySet()) { 
                        try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", init.key.view, init.key.seq, node.nodeId, eall.getKey(), req.getClientId(), req.getReqSeq(), entry.prePrepare.getRequestDigest().toByteArray(), eall.getValue(), "rebroadcast"); } catch (Exception ignored) {} 
                    }
                    });
                    replicaContext.outbound.execute(() -> {
                    try { Thread.sleep(900); } catch (InterruptedException ignored) {}
                    var en = replicaContext.state.get(init.key.view, init.key.seq);
                    if (en == null || en.executed || en.committedFlag || en.prepared) return;
                    if (replicaContext.state.currentView() != init.key.view) return;
                    var res = replicaContext.sender.sendPrePrepareToAll(entry.prePrepare, RPC_DEADLINE_MS);
                    if (res != null) for (var eall : res.entrySet()) { 
                        try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", init.key.view, init.key.seq, node.nodeId, eall.getKey(), req.getClientId(), req.getReqSeq(), entry.prePrepare.getRequestDigest().toByteArray(), eall.getValue(), "rebroadcast"); } catch (Exception ignored) {} 
                    }
                    });
                    replicaContext.outbound.execute(() -> {
                    try { Thread.sleep(1500); } catch (InterruptedException ignored) {}
                    var en = replicaContext.state.get(init.key.view, init.key.seq);
                    if (en == null || en.executed || en.committedFlag || en.prepared) return;
                    if (replicaContext.state.currentView() != init.key.view) return;
                    Map<String, String> results4 = replicaContext.sender.sendPrePrepareToAll(entry.prePrepare, RPC_DEADLINE_MS);
                    if (results4 != null) {
                        for (var e4 : results4.entrySet()) {
                            if (!"OK".equals(e4.getValue())) {
                                log.warn("SEND PrePrepare v={} s={} target={} status={} on {}", init.key.view, init.key.seq, e4.getKey(), e4.getValue(), node.nodeId);
                            }
                            try { replicaContext.state.recordEvent("OUT", "pbft.PrePrepare", init.key.view, init.key.seq, node.nodeId, e4.getKey(), req.getClientId(), req.getReqSeq(), entry.prePrepare.getRequestDigest().toByteArray(), e4.getValue(), "rebroadcast"); } catch (Exception ignored) {}
                        }
                    }
                    });
                }
            }
            entry.committed.get(15000, TimeUnit.MILLISECONDS);
            SignedMessage replySignedMsg = awaitSignedFinalReply(entry, RPC_DEADLINE_MS);
            if (replySignedMsg == null) {
                resp.onError(Status.DEADLINE_EXCEEDED.withDescription("final reply unavailable").asRuntimeException());
                return;
            }
            try { replicaContext.state.recordEvent("OUT", "pbft.ClientReply", replicaContext.state.currentView(), 0L, node.nodeId, req.getClientId(), req.getClientId(), req.getReqSeq(), null, "final", "submit"); } catch (Exception ignored) {}
            resp.onNext(replySignedMsg);
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(Status.DEADLINE_EXCEEDED.withDescription("commit wait failed: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void readOnly(SignedMessage env, StreamObserver<SignedMessage> resp) {
        if (!replicaContext.liveSelf) { resp.onError(Status.UNAVAILABLE.withDescription("replica inactive").asRuntimeException()); return; }
        if (replicaContext.sender.isByzantine() &&
                (replicaContext.sender.hasAttackType("crash") || replicaContext.sender.hasAttackType("failstop"))) {
            resp.onError(Status.UNAVAILABLE.withDescription("replica unavailable").asRuntimeException());
            return;
        }

        var tr = MessageDecoder.decodeClientRequest(env, node.keyStore);
        if (!tr.ok()) { resp.onError(GrpcStatusUtil.mapStatus(tr.validation()).asRuntimeException()); return; }
        if (!tr.message().getOperation().hasBalance()) { resp.onError(Status.INVALID_ARGUMENT.withDescription("readOnly requires Balance op").asRuntimeException()); return; }
        String clientIdRo = tr.message().getClientId();
        long tLogicalRo = tr.message().getReqSeq();
        if (tLogicalRo > 1) {
            var depKey = replicaContext.state.findByClientReq(clientIdRo, tLogicalRo - 1);
            if (depKey != null) {
                long waitNanos = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(Math.max(1L, RPC_DEADLINE_MS - 200L));
                long dl = System.nanoTime() + waitNanos;
                while (System.nanoTime() < dl) {
                    var depEntry = replicaContext.state.get(depKey.view, depKey.seq);
                    if (depEntry != null && depEntry.executed) break;
                    try { Thread.sleep(10); } catch (InterruptedException ignored) {}
                }
                var depEntry2 = replicaContext.state.get(depKey.view, depKey.seq);
                if (depEntry2 != null && !depEntry2.executed) { resp.onError(Status.DEADLINE_EXCEEDED.withDescription("prior request not executed").asRuntimeException()); return; }
            }
        }
        var balOp = tr.message().getOperation().getBalance();
        long bal = Boolean.getBoolean("pbft.benchmark.smallbank.enabled")
                ? replicaContext.db.getTotal(balOp.getAccount())
                : replicaContext.db.getBalance(balOp.getAccount());
        long vnow = replicaContext.state.currentView();
        long tLogical = tr.message().getReqSeq();
        log.info("START-RO v={} t={} op=Balance({}) on {}", vnow, tLogical, balOp.getAccount(), node.nodeId);
        ClientReply reply = ClientReply.newBuilder()
                .setClientId(tr.message().getClientId())
                .setReqSeq(tr.message().getReqSeq())
                .setView(replicaContext.state.currentView())
                .setReplicaId(node.nodeId)
                .setBalance(bal)
                .build();
        try {
            SignedMessage replySignedMsg = SignedMessagePacker.encode(
                RequestType.CLIENT_REPLY,
                PbftMsgTypes.CLIENT_REPLY,
                reply,
                node.nodeId,
                replicaContext.state.currentView(),
                0,
                node.sk);
            if (replicaContext.sender.isByzantine() && replicaContext.sender.hasAttackType("sign")) {
                replySignedMsg = replySignedMsg.toBuilder()
                        .setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0}))
                        .build();
            }
            try { replicaContext.state.recordEvent("OUT", "pbft.ClientReply", replicaContext.state.currentView(), 0L, node.nodeId, tr.message().getClientId(), tr.message().getClientId(), tr.message().getReqSeq(), null, Long.toString(bal), "readOnly"); } catch (Exception ignored) {}
            resp.onNext(replySignedMsg); resp.onCompleted();
        } catch (GeneralSecurityException e) {
            resp.onError(Status.INTERNAL.withDescription("signing failed: " + e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void healthCheck(HealthCheckRequest req, StreamObserver<HealthCheckReply> resp) {
        try {
            resp.onNext(HealthCheckReply.newBuilder().setStatus("OK").build());
            resp.onCompleted();
        } catch (Exception e) {
            resp.onError(Status.INTERNAL.withDescription("health failed").asRuntimeException());
        }
    }

    private void scheduleSubsetPrePrepare(ReplicaState.EntryKey key,
                                          java.util.Set<String> targets,
                                          long delayMs) {
        if (key == null || targets == null || targets.isEmpty()) return;
        replicaContext.outbound.execute(() -> {
            try { Thread.sleep(delayMs); } catch (InterruptedException ignored) {}
            var entry = replicaContext.state.get(key.view, key.seq);
            if (entry == null) return;
            if (entry.executed || entry.committedFlag || entry.prepared) return;
            if (replicaContext.state.currentView() != key.view) return;
            if (entry.prePrepare == null) return;
            replicaContext.sender.sendPrePrepareToSubset(entry.prePrepare, RPC_DEADLINE_MS, targets);
        });
    }

    private SignedMessage awaitSignedFinalReply(ReplicaState.Entry entry, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            SignedMessage signedMsg = entry.finalReplySignedMsg;
            if (signedMsg != null) {
                return signedMsg;
            }
            if (entry.executed && entry.finalReplySignedMsg == null) {
                break;
            }
            try { Thread.sleep(5); } catch (InterruptedException ignored) {}
        }
        return entry.finalReplySignedMsg;
    }

    private record PrimaryInitResult(ReplicaState.EntryKey key, boolean broadcastNeeded, ReplicaState.EntryKey key2) {}
}
