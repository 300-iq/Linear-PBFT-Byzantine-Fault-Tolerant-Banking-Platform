package pbft.replica.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pbft.replica.rpc.RpcSupport;
import pbft.proto.ViewChange;
import pbft.common.validation.SignedMessagePacker;
import pbft.common.validation.PbftMsgTypes;
import pbft.common.crypto.RequestType;
import pbft.proto.SignedMessage;

import java.util.Objects;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public final class ReplicaTimers implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ReplicaTimers.class);
    private static final long VC_BROADCAST_DEADLINE_MS = Math.max(4000L, Long.getLong("pbft.rpc.deadline_ms", 8000L));

    private final ReplicaContext ctx;
    private final ScheduledExecutorService scheduler;
    private final long baseTimeout;
    private final long maxTimeoutMillis;
    private final boolean progressMonitorEnabled;

    private final Object lock = new Object();
    private ScheduledFuture<?> progressMonitor;
    private volatile long lastPulseNanos;

    private ScheduledFuture<?> progressTimer;
    private ScheduledFuture<?> vcWaitTimer;
    private long vcWaitTargetView = 0;
    private long vcWaitTimeoutMillis;
    private long lastProgressView = 0;
    private boolean progressArmed = false;
    private long lastSentVcView = 0;
    private final Map<Long, Set<String>> vcByView = new HashMap<>();

    public ReplicaTimers(ReplicaContext ctx, long baseTimeoutMillis) {
        this.ctx = Objects.requireNonNull(ctx);
        this.baseTimeout = baseTimeoutMillis;
        this.maxTimeoutMillis = Math.max(baseTimeoutMillis * 8, baseTimeoutMillis);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new TF(ctx.node.nodeId + "-timers"));
        String wd = System.getProperty("pbft.timers.progressMonitorTimer");
        this.progressMonitorEnabled = wd != null && ("1".equals(wd) || "true".equalsIgnoreCase(wd) || "yes".equalsIgnoreCase(wd));
    }

    public void pulse(String reason) {
        if (!progressMonitorEnabled) return;
        lastPulseNanos = System.nanoTime();
        synchronized (lock) {
            if (progressMonitor != null) {
                progressMonitor.cancel(false);
                progressMonitor = null;
            }
            long delay = baseTimeout;
            progressMonitor = scheduler.schedule(this::onTimeout, delay, TimeUnit.MILLISECONDS);
        }
    }

    private void onTimeout() {
        long sinceMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastPulseNanos);
        long v = ctx.state.currentView();
        String primary = pbft.replica.rpc.RpcSupport.ViewUtil.primaryId(v, ctx.node.n);
        log.warn("PROGRESS TIMER timeout after {}ms in view {} (primary={}) on {} — would trigger ViewChange to {}",
                sinceMs, v, primary, ctx.node.nodeId, v + 1);
    }

    public void onViewStart(long view) {
        synchronized (lock) {
            cancel(progressTimer);
            cancel(vcWaitTimer);
            progressArmed = false;
            lastProgressView = view;
            vcWaitTargetView = 0;
            vcWaitTimeoutMillis = baseTimeout;
        }
        ensurePrimaryLive(view);
    }
    public void resetAll(long view) {
        synchronized (lock) {
            cancel(progressMonitor);
            cancel(progressTimer);
            cancel(vcWaitTimer);
            progressMonitor = null;
            progressTimer = null;
            vcWaitTimer = null;
            lastPulseNanos = 0L;
            progressArmed = false;
            lastProgressView = view;
            vcWaitTargetView = 0;
            vcWaitTimeoutMillis = baseTimeout;
            lastSentVcView = 0;
            vcByView.clear();
        }
    }

    public void onPrePrepareAccepted(long view, long seq) {
        String primary = RpcSupport.ViewUtil.primaryId(view, ctx.node.n);
        if (ctx.node.nodeId.equals(primary)) return;

        synchronized (lock) {
            if (vcWaitTargetView == view) {
                cancel(vcWaitTimer);
                vcWaitTargetView = 0;
                vcWaitTimeoutMillis = baseTimeout;
                log.debug("Timers: NV wait satisfied by PrePrepare(v={}) on {}", view, ctx.node.nodeId);
            }
        }

        var en = ctx.state.get(view, seq);
        if (en != null && en.executed) return;

        if (en != null) {
            String cid = null; long t = 0L;
            if (en.clientRequest != null) { cid = en.clientRequest.getClientId(); t = en.clientRequest.getReqSeq(); }
            else if (en.prePrepare != null) { cid = en.prePrepare.getClientId(); t = en.prePrepare.getReqSeq(); }
            if (cid != null && !cid.isEmpty() && t != 0L && ctx.state.hasExecuted(cid, t)) {
                return;
            }
        }

        long nextExec = ctx.state.nextExecSeq();
        if (seq != nextExec) {
            return;
        }
        synchronized (lock) {
            cancel(progressTimer);
            progressTimer = scheduler.schedule(() -> onProgressTimeout(view), baseTimeout, TimeUnit.MILLISECONDS);
            progressArmed = true;
            lastProgressView = view;
            log.debug("Timers: armed backup progress timer for v={} on {}", view, ctx.node.nodeId);
        }
    }


    public void onExecuted() {
        synchronized (lock) {
            cancel(progressTimer);
            progressArmed = false;
            long view = ctx.state.currentView();

            if (vcWaitTargetView == view) {
                cancel(vcWaitTimer);
                vcWaitTargetView = 0;
                vcWaitTimeoutMillis = baseTimeout;
            }

            boolean waiting = false;
            for (var e : ctx.state.snapshotLog().entrySet()) {
                var k = e.getKey(); var en = e.getValue();
                if (k.view == view && en.prePrepare != null && !en.executed) {

                    String cid = null; long t = 0L;
                    if (en.clientRequest != null) { cid = en.clientRequest.getClientId(); t = en.clientRequest.getReqSeq(); }
                    else if (en.prePrepare != null) { cid = en.prePrepare.getClientId(); t = en.prePrepare.getReqSeq(); }
                    if (cid != null && !cid.isEmpty() && t != 0L && ctx.state.hasExecuted(cid, t)) {
                        continue;
                    }
                    waiting = true; break;
                }
            }
            if (waiting) {
                cancel(progressTimer);
                progressTimer = scheduler.schedule(() -> onProgressTimeout(view), baseTimeout, TimeUnit.MILLISECONDS);
                progressArmed = true;
                lastProgressView = view;
                log.debug("Timers: restarted progress timer after execution for v={} on {}", view, ctx.node.nodeId);
            }
        }
    }

    private void onProgressTimeout(long view) {

        long current = ctx.state.currentView();
        String primary = RpcSupport.ViewUtil.primaryId(view, ctx.node.n);
        if (current != view || ctx.node.nodeId.equals(primary)) return;
        log.warn("Timers: progress timeout in view {} on {} — initiating ViewChange to {}", view, ctx.node.nodeId, view + 1);
        beginViewChange(view + 1);
    }

    public void onViewChangeReceived(long vPrime, String signerId) {
        synchronized (lock) {
            vcByView.computeIfAbsent(vPrime, k -> new HashSet<>()).add(signerId);
            int f = ctx.node.f;
            long current = ctx.state.currentView();

            if (vPrime > current) {
                long smallest = Long.MAX_VALUE;
                for (var e : vcByView.entrySet()) {
                    if (e.getKey() > current && e.getValue().size() >= f + 1) {
                        smallest = Math.min(smallest, e.getKey());
                    }
                }
                if (smallest != Long.MAX_VALUE && (lastSentVcView == 0 || smallest < lastSentVcView)) {
                    log.info("Timers: expedite rule met (f+1 VC) — sending ViewChange for v'={} on {}", smallest, ctx.node.nodeId);
                    beginViewChange(smallest);
                }
            }

            if (lastSentVcView == vPrime) {
                int count = vcByView.getOrDefault(vPrime, Set.of()).size();
                if (count >= (2 * f + 1) && (vcWaitTimer == null || vcWaitTargetView != vPrime)) {
                    startNewViewWait(vPrime);
                }
            }
        }
    }


    public void onNewViewAccepted(long vPrime) {
        synchronized (lock) {
            if (vcWaitTargetView == vPrime) {
                log.info("Timers: NewView accepted for v'={} on {} — cancelling NV wait", vPrime, ctx.node.nodeId);
                cancel(vcWaitTimer);
                vcWaitTargetView = 0;
                vcWaitTimeoutMillis = baseTimeout;
            }

            pulse("newview-accepted");

            onViewStart(vPrime);
        }
    }

    public void onClientRequestAwaiting(long view) {
        String primary = RpcSupport.ViewUtil.primaryId(view, ctx.node.n);
        if (ctx.node.nodeId.equals(primary)) return;
        synchronized (lock) {
            cancel(progressTimer);
            progressTimer = scheduler.schedule(() -> onProgressTimeout(view), baseTimeout, TimeUnit.MILLISECONDS);
            progressArmed = true;
            lastProgressView = view;
            log.debug("Timers: armed client-wait timer for v={} on {}", view, ctx.node.nodeId);
        }
    }


    public void forceViewChange(long targetView) {
        if (targetView <= 0) return;
        beginViewChange(targetView);
    }

    private void ensurePrimaryLive(long view) {
        String primary = RpcSupport.ViewUtil.primaryId(view, ctx.node.n);
        boolean primaryLive;
        if (primary.equals(ctx.node.nodeId)) {
            primaryLive = ctx.liveSelf;
        } else {
            primaryLive = ctx.peers.isLive(primary);
        }
        if (primaryLive) return;
        long candidate = view;
        for (int i = 1; i <= ctx.node.n; i++) {
            long next = view + i;
            String nextPrimary = RpcSupport.ViewUtil.primaryId(next, ctx.node.n);
            boolean nextLive = nextPrimary.equals(ctx.node.nodeId) ? ctx.liveSelf : ctx.peers.isLive(nextPrimary);
            if (nextLive) {
                candidate = next;
                break;
            }
        }
        if (candidate > view) {
            log.info("Timers: primary {} not live for view {} on {} — forcing ViewChange to {}", primary, view, ctx.node.nodeId, candidate);
            forceViewChange(candidate);
        }
    }

    private void beginViewChange(long targetView) {
        synchronized (lock) {
            lastSentVcView = targetView;
            vcByView.computeIfAbsent(targetView, k -> new HashSet<>()).add(ctx.node.nodeId);

            cancel(progressTimer);
            progressArmed = false;

            if (ctx.engine != null) {
                ctx.engine.submit("BeginViewChange(" + targetView + ")", () -> {
                    ctx.state.viewChangeActive = true;
                    ctx.state.pendingNewView = targetView;
                    long s = ctx.state.lastStableSeq;
                    var pset = ctx.state.exportPreparedEntries(s);
                    ViewChange.Builder vcb = ViewChange.newBuilder()
                            .setNewView(targetView)
                            .setLastStableSeq(s)
                            .addAllPset(pset)
                            .setReplicaId(ctx.node.nodeId);
                    if (Boolean.getBoolean("pbft.checkpoints.enabled")) {
                        try {
                            vcb.setCheckpointDigest(ctx.state.stableCheckpointDigestBytes())
                               .addAllCheckpointMsgs(ctx.state.stableCheckpointCertificate());
                        } catch (Exception ignore) {}
                    }
                    ViewChange vc = vcb.build();
                    SignedMessage selfVcSigned = null;
                    try {
                        selfVcSigned = SignedMessagePacker.encode(RequestType.VIEW_CHANGE, PbftMsgTypes.VIEW_CHANGE, vc, ctx.node.nodeId, targetView, 0, ctx.node.sk);

                        if (selfVcSigned != null && ctx.sender.isByzantine() && ctx.sender.hasAttackType("sign")) {
                            selfVcSigned = selfVcSigned.toBuilder()
                                    .setSignature(com.google.protobuf.ByteString.copyFrom(new byte[]{0}))
                                    .build();
                        }
                    } catch (Exception ex) {
                        log.error("Failed to sign ViewChange v'={} on {}: {}", targetView, ctx.node.nodeId, ex.getMessage());
                    }
                    if (selfVcSigned != null) {
                        ctx.state.recordViewChange(targetView, ctx.node.nodeId, selfVcSigned);
                    }
            ctx.outbound.execute(() -> {
                var results = ctx.sender.broadcastViewChange(vc, VC_BROADCAST_DEADLINE_MS);
                        if (results != null) {
                            for (var e : results.entrySet()) {
                                if (!"OK".equals(e.getValue())) {
                                    log.warn("SEND ViewChange v'={} target={} status={} on {}", targetView, e.getKey(), e.getValue(), ctx.node.nodeId);
                                }
                                try { ctx.state.recordEvent("OUT", "pbft.ViewChange", targetView, 0L, ctx.node.nodeId, e.getKey(), "", 0L, null, e.getValue(), ""); } catch (Exception ignored) {}
                            }
                        }
                    });
                });
            } else {

                ctx.state.viewChangeActive = true;
                ctx.state.pendingNewView = targetView;
            }
            startNewViewWait(targetView);
        }
    }

    private void startNewViewWait(long vPrime) {
        cancel(vcWaitTimer);
        vcWaitTargetView = vPrime;
        if (vcWaitTimeoutMillis <= 0) vcWaitTimeoutMillis = baseTimeout;
        long delay = vcWaitTimeoutMillis;
        vcWaitTimer = scheduler.schedule(() -> onNewViewWaitTimeout(vPrime), delay, TimeUnit.MILLISECONDS);
        log.info("Timers: waiting for NewView v'={} with T={}ms on {}", vPrime, delay, ctx.node.nodeId);
    }

    private void onNewViewWaitTimeout(long vPrime) {
        synchronized (lock) {
            if (vcWaitTargetView != vPrime) return;
            long next = vPrime + 1;
            vcWaitTimeoutMillis = Math.min(vcWaitTimeoutMillis * 2, maxTimeoutMillis);
            log.warn("Timers: NewView wait expired for v'={} on {} — escalating to v'={} with T={}ms", vPrime, ctx.node.nodeId, next, vcWaitTimeoutMillis);
            beginViewChange(next);
        }
    }

    private static void cancel(ScheduledFuture<?> f) {
        if (f != null) f.cancel(false);
    }

    @Override
    public void close() {
        synchronized (lock) {
            cancel(progressMonitor);
            cancel(progressTimer);
            cancel(vcWaitTimer);
            progressMonitor = null;
            progressTimer = null;
            vcWaitTimer = null;
        }
        scheduler.shutdownNow();
    }

    private static final class TF implements ThreadFactory {
        private final String base;
        TF(String base) { this.base = base; }
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, base);
            t.setDaemon(true);
            return t;
        }
    }
}
