package pbft.replica.rpc;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pbft.proto.AdminServiceGrpc;
import pbft.proto.BalanceEntry;
import pbft.proto.ClientRequest;
import pbft.proto.Empty;
import pbft.proto.LogEntry;
import pbft.proto.Operation;
import pbft.proto.PrintDBReply;
import pbft.proto.PrintLogReply;
import pbft.proto.PrintCompleteLogReply;
import pbft.proto.CompleteLogEntry;
import pbft.proto.SeqQuery;
import pbft.proto.PrintCheckpointReply;
import pbft.proto.StatusEntry;
import pbft.proto.StatusReply;
import pbft.proto.ViewLog;
import pbft.proto.LiveNodesRequest;
import pbft.replica.core.ConsensusEngine;
import pbft.replica.core.ReplicaContext;
import pbft.replica.core.ReplicaState;
import pbft.common.util.Hex;
import com.google.protobuf.ByteString;
import pbft.common.crypto.Digests;
import java.nio.charset.StandardCharsets;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class AdminRpcService extends AdminServiceGrpc.AdminServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(AdminRpcService.class);
    private final ReplicaContext ctx;
    private final ConsensusEngine engine;

    public AdminRpcService(ConsensusEngine engine, ReplicaContext ctx) { this.ctx = ctx; this.engine = engine; }

    @Override
    public void printDB(Empty req, StreamObserver<PrintDBReply> resp) {
        var map = ctx.db.snapshot();
        boolean sb = Boolean.getBoolean("pbft.benchmark.smallbank.enabled");
        java.util.ArrayList<BalanceEntry> balances = new java.util.ArrayList<>();
        for (char c = 'A'; c <= 'J'; c++) {
            String k = String.valueOf(c);
            long v;
            if (sb) {
                long ck = map.getOrDefault("checking:" + k, map.getOrDefault(k, 0L));
                long sv = map.getOrDefault("savings:" + k, 0L);
                v = ck + sv;
            } else {
                Long cv = map.get("checking:" + k);
                if (cv != null) {
                    v = cv;
                } else {
                    v = map.getOrDefault(k, 0L);
                }
            }
            balances.add(BalanceEntry.newBuilder().setAccount(k).setBalance(v).build());
        }
        PrintDBReply reply = PrintDBReply.newBuilder().addAllBalances(balances).build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void setAttack(pbft.proto.AttackSpec req, StreamObserver<Empty> resp) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        engine.submit("AdminSetAttack", () -> {
            try {
                java.util.Set<String> types = new java.util.HashSet<>(req.getTypesList());
                java.util.Set<String> dark = new java.util.HashSet<>(req.getDarkTargetsList());
                java.util.Set<String> equiv = new java.util.HashSet<>(req.getEquivTargetsList());
                long delay;
                if (types.contains("time")) {
                    try {
                        delay = Long.getLong("pbft.attacks.time.delay_ms", 100L);
                    } catch (Exception ignored) {
                        delay = 100L;
                    }
                } else {
                    delay = 0L;
                }
                ctx.sender.setAttack(req.getByzantine(), types, dark, equiv, delay);

                boolean failstop = req.getByzantine() && (types.contains("failstop") || types.contains("fail-stop"));
                if (failstop) {
                    ctx.sender.setLiveEnabled(false);
                } else {
                    ctx.sender.setLiveEnabled(true);
                }
            } catch (Exception ignored) {}
            done.complete(null);
        });
        try { done.get(2, TimeUnit.SECONDS); } catch (Exception ignored) {}
        resp.onNext(Empty.getDefaultInstance()); resp.onCompleted();
    }

    @Override
    public void printLog(Empty req, StreamObserver<PrintLogReply> resp) {
        var snap = ctx.state.snapshotLog();
        var entries = snap.entrySet().stream()
                .sorted(Comparator
                        .comparingLong((Map.Entry<ReplicaState.EntryKey, ReplicaState.Entry> e) -> e.getKey().view)
                        .thenComparingLong(e -> e.getKey().seq))
                .map(e -> toLogEntry(e.getKey().view, e.getKey().seq, e.getValue()))
                .collect(Collectors.toList());
        PrintLogReply reply = PrintLogReply.newBuilder().addAllEntries(entries).build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void printCompleteLog(Empty req, StreamObserver<PrintCompleteLogReply> resp) {
        java.util.List<ReplicaState.CompleteEvent> raw = ctx.state.snapshotCompleteLog();
        raw.sort(java.util.Comparator.comparingLong(e -> e.tsMillis));
        java.util.List<CompleteLogEntry> entries = new java.util.ArrayList<>(raw.size());
        for (var e : raw) {
            entries.add(toCompleteEntry(e));
        }
        var snap = ctx.state.snapshotLog();
        var stateEntries = snap.entrySet().stream()
                .sorted(Comparator
                        .comparingLong((Map.Entry<ReplicaState.EntryKey, ReplicaState.Entry> e) -> e.getKey().view)
                        .thenComparingLong(e -> e.getKey().seq))
                .map(e -> toLogEntry(e.getKey().view, e.getKey().seq, e.getValue()))
                .collect(Collectors.toList());
        PrintCompleteLogReply reply = PrintCompleteLogReply.newBuilder()
                .addAllEntries(entries)
                .addAllStateEntries(stateEntries)
                .build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void printCheckpoint(Empty req, StreamObserver<PrintCheckpointReply> resp) {
        boolean enabled = Boolean.getBoolean("pbft.checkpoints.enabled");
        long period;
        try { period = Math.max(1L, Long.getLong("pbft.checkpoints.period", 100L)); }
        catch (Exception ignored) { period = 100L; }

        long s = ctx.state.lastStableSeq;
        ByteString dg;
        try { dg = ctx.state.stableCheckpointDigestBytes(); } catch (Exception ignored) { dg = ByteString.EMPTY; }
        int signers;
        java.util.List<String> signerIds = new java.util.ArrayList<>();
        try {
            var cert = ctx.state.stableCheckpointCertificate();
            signers = cert.size();
            for (var env : cert) {
                try { signerIds.add(env.getSignerId()); } catch (Exception ignored) {}
            }
        } catch (Exception ignored) { signers = 0; }
        boolean hasLocal = false;
        try { hasLocal = (s > 0) && (ctx.state.localCheckpointSnapshot(s) != null); } catch (Exception ignored) {}

        java.util.List<LogEntry> entries = new java.util.ArrayList<>();
        try {
            if (s > 0 && period > 0) {
                long start = Math.max(1L, s - period + 1);
                var snap = ctx.state.snapshotLog();
                snap.entrySet().stream()
                        .filter(e -> {
                            long seq = e.getKey().seq;
                            if (seq < start || seq > s) return false;
                            ReplicaState.Entry en = e.getValue();
                            return en != null && en.executed;
                        })
                        .sorted(Comparator
                                .comparingLong((Map.Entry<ReplicaState.EntryKey, ReplicaState.Entry> e) -> e.getKey().view)
                                .thenComparingLong(e -> e.getKey().seq))
                        .forEach(e -> entries.add(toLogEntry(e.getKey().view, e.getKey().seq, e.getValue())));
            }
        } catch (Exception ignored) {}

        PrintCheckpointReply reply = PrintCheckpointReply.newBuilder()
                .setEnabled(enabled)
                .setPeriod(period)
                .setLastStableSeq(s)
                .setLastStableDigest(dg)
                .setStableSigners(signers)
                .addAllStableSignerIds(signerIds)
                .setHasLocalSnapshot(hasLocal)
                .addAllCheckpointEntries(entries)
                .build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void printStatus(SeqQuery req, StreamObserver<StatusReply> resp) {
        Map<String, String> statusMap = new HashMap<>();
        StatusEntry local = localStatusEntry(req.getView(), req.getSeq());
        statusMap.put(local.getReplicaId(), local.getStatus());

        if (req.getAggregate()) {
            for (var peer : ctx.peers.adminStubs().entrySet()) {
                var peerReq = SeqQuery.newBuilder()
                        .setView(req.getView())
                        .setSeq(req.getSeq())
                        .setAggregate(false)
                        .build();
                try {
                    StatusReply r = peer.getValue()
                            .withDeadlineAfter(2, TimeUnit.SECONDS)
                            .printStatus(peerReq);
                    for (var s : r.getStatusesList()) {
                        statusMap.put(s.getReplicaId(), s.getStatus());
                    }
                } catch (StatusRuntimeException ex) {
                    statusMap.put(peer.getKey(), "ERR");
                }
            }
        }

        List<StatusEntry> statuses = statusMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(e -> StatusEntry.newBuilder()
                        .setReplicaId(e.getKey())
                        .setStatus(e.getValue())
                        .build())
                .collect(Collectors.toList());
        StatusReply reply = StatusReply.newBuilder().addAllStatuses(statuses).build();
        resp.onNext(reply);
        resp.onCompleted();
    }

    @Override
    public void printView(Empty req, StreamObserver<ViewLog> resp) {
        var list = ctx.state.snapshotViewLog();
        ViewLog.Builder b = ViewLog.newBuilder().addAllNewViews(list);
        var last = ctx.state.snapshotLastNewView();
        if (last != null) {
            b.setHasLast(true)
             .setLastView(last.getNewView())
             .setS(ctx.state.lastNewViewS())
             .setH(ctx.state.lastNewViewH())
             .setLastNewView(last);
        }
        var hist = ctx.state.snapshotNewViewHistory();
        var sHist = ctx.state.snapshotSCovHistory();
        var hHist = ctx.state.snapshotHCovHistory();
        if (!hist.isEmpty()) {
            b.addAllNewViewsFull(hist).addAllSCov(sHist).addAllHCov(hHist);
        }
        resp.onNext(b.build()); resp.onCompleted();
    }

    @Override
    public void reset(Empty req, StreamObserver<Empty> resp) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        engine.submit("AdminReset", () -> {
            try { ctx.sender.setLiveEnabled(false); } catch (Exception ignored) {}
            try { ctx.sender.setAttack(false, java.util.Collections.emptySet(), java.util.Collections.emptySet(), java.util.Collections.emptySet(), 0L); } catch (Exception ignored) {}
            ctx.liveSelf = true;
            try { ctx.peers.setLivePeers(ctx.peers.peerIds()); } catch (Exception ignored) {}
            ctx.db.reset();
            ctx.state.resetForNextSet(ctx.node.initialView);
            ctx.timers.resetAll(ctx.node.initialView);
            done.complete(null);
        });
        try { done.get(3, TimeUnit.SECONDS); } catch (Exception ignored) {}
        resp.onNext(Empty.getDefaultInstance()); resp.onCompleted();
    }

    @Override
    public void setLiveNodes(LiveNodesRequest req, StreamObserver<Empty> resp) {
        var ids = req.getNodeIdsList();
        CompletableFuture<Void> done = new CompletableFuture<>();
        engine.submit("AdminSetLiveNodes", () -> {
            ctx.peers.setLivePeers(ids);
            ctx.liveSelf = ids.contains(ctx.node.nodeId);
            ctx.sender.setLiveEnabled(ctx.liveSelf);
            if (ctx.liveSelf && ctx.timers != null) {
                long currentView = ctx.state.currentView();
                String currentPrimary = RpcSupport.ViewUtil.primaryId(currentView, ctx.node.n);
                if (!ids.contains(currentPrimary)) {
                    long targetView = currentView + 1;
                    for (int i = 1; i <= ctx.node.n; i++) {
                        long candidate = currentView + i;
                        String candidatePrimary = RpcSupport.ViewUtil.primaryId(candidate, ctx.node.n);
                        if (ids.contains(candidatePrimary)) {
                            targetView = candidate;
                            break;
                        }
                    }
                    long finalTargetView = targetView;
                }
            }
            done.complete(null);
        });
        try { done.get(3, TimeUnit.SECONDS); } catch (Exception ignored) {}
        resp.onNext(Empty.getDefaultInstance()); resp.onCompleted();
    }

    private String statusOf(ReplicaState.Entry e) {
        if (e.executed) return "E";
        if (e.committedFlag) return "C";
        if (e.prepared) return "P";
        if (e.prePrepared) return "PP";
        return "X";
    }

    private StatusEntry localStatusEntry(long view, long seq) {
        String status = "X";
        long effectiveView = view;
        ReplicaState.Entry entry = null;
        if (seq > 0) {
            if (view > 0) {
                entry = ctx.state.get(view, seq);
            } else {
                var latestKey = ctx.state.latestEntryKeyForSeq(seq);
                if (latestKey != null) {
                    effectiveView = latestKey.view;
                    entry = ctx.state.get(latestKey.view, latestKey.seq);
                }
            }
        }
        if (entry != null) {
            status = statusOf(entry);
            if (view <= 0) {
                status = status + " (v=" + effectiveView + ")";
            }
        }
        return StatusEntry.newBuilder()
                .setReplicaId(ctx.node.nodeId)
                .setStatus(status)
                .build();
    }

    private LogEntry toLogEntry(long view, long seq, ReplicaState.Entry entry) {
        String status = statusOf(entry);
        String clientId = "";
        long reqSeq = 0;
        String operation = "UNKNOWN";

        if (entry.clientRequest != null) {
            clientId = entry.clientRequest.getClientId();
            reqSeq = entry.clientRequest.getReqSeq();
            operation = describeOperation(entry.clientRequest.getOperation());
        } else if (entry.prePrepare != null) {
            clientId = entry.prePrepare.getClientId();
            reqSeq = entry.prePrepare.getReqSeq();
            if (entry.prePrepare.hasClientRequest()) {
                try {
                    ClientRequest embedded = ClientRequest.parseFrom(entry.prePrepare.getClientRequest().getPayload());
                    operation = describeOperation(embedded.getOperation());
                } catch (Exception ignored) {
                }
            }
        }

        String digest = Hex.toHexOrEmpty(entry.requestDigest);

        if ("UNKNOWN".equals(operation)) {
            try {
                byte[] nullDigest = Digests.sha256("pbft:null".getBytes(StandardCharsets.UTF_8));
                if (entry.requestDigest != null && java.util.Arrays.equals(entry.requestDigest, nullDigest)) {
                    operation = "NO-OP";
                }
            } catch (Exception ignored) {}
        }

        LogEntry.Builder builder = LogEntry.newBuilder()
                .setView(view)
                .setSeq(seq)
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setStatus(status)
                .setRequestDigest(digest)
                .setOperation(operation)
                .setPrePrepared(entry.prePrepared)
                .setPrepared(entry.prepared)
                .setCommitted(entry.committedFlag)
                .setExecuted(entry.executed);
        return builder.build();
    }

    private CompleteLogEntry toCompleteEntry(ReplicaState.CompleteEvent e) {
        String dg = Hex.toHexOrEmpty(e.requestDigest);
        CompleteLogEntry.Builder b = CompleteLogEntry.newBuilder()
                .setTsMillis(e.tsMillis)
                .setDirection(e.direction == null ? "" : e.direction)
                .setType(e.type == null ? "" : e.type)
                .setView(e.view)
                .setSeq(e.seq)
                .setFrom(e.from == null ? "" : e.from)
                .setTo(e.to == null ? "" : e.to)
                .setClientId(e.clientId == null ? "" : e.clientId)
                .setReqSeq(e.reqSeq)
                .setRequestDigest(dg == null ? "" : dg)
                .setStatus(e.status == null ? "" : e.status)
                .setNotes(e.notes == null ? "" : e.notes);
        return b.build();
    }

    private String describeOperation(Operation op) {
        if (op == null) return "UNKNOWN";
        if (op.hasTransfer()) {
            var t = op.getTransfer();
            return "Transfer(" + t.getFrom() + "->" + t.getTo() + ", amt=" + t.getAmount() + ")";
        }
        if (op.hasBalance()) {
            return "Balance(" + op.getBalance().getAccount() + ")";
        }
        if (op.hasDepositChecking()) {
            var o = op.getDepositChecking();
            return "DepositChecking(" + o.getAccount() + ", amt=" + o.getAmount() + ")";
        }
        if (op.hasTransactSavings()) {
            var o = op.getTransactSavings();
            return "TransactSavings(" + o.getAccount() + ", amt=" + o.getAmount() + ")";
        }
        if (op.hasWriteCheck()) {
            var o = op.getWriteCheck();
            return "WriteCheck(" + o.getAccount() + ", amt=" + o.getAmount() + ")";
        }
        if (op.hasSendPayment()) {
            var o = op.getSendPayment();
            return "SendPayment(" + o.getSrc() + "->" + o.getDst() + ", amt=" + o.getAmount() + ")";
        }
        if (op.hasAmalgamate()) {
            var o = op.getAmalgamate();
            return "Amalgamate(" + o.getSrc() + "->" + o.getDst() + ")";
        }
        return "UNKNOWN";
    }

    
}
