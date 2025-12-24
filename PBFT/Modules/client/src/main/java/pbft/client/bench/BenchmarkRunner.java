package pbft.client.bench;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import pbft.common.config.ClusterConfig;
import pbft.common.config.KeyStore;
import pbft.common.crypto.RequestType;
import pbft.common.validation.MessageDecoder;
import pbft.common.validation.SignedMessagePacker;
import pbft.common.validation.PbftMsgTypes;
import pbft.proto.*;
import pbft.common.util.Hex;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class BenchmarkRunner implements AutoCloseable {
    private final ClusterConfig clusterConfig;
    private final Map<String, ManagedChannel> nodeChannels = new HashMap<>();
    private final Map<String, ClientServiceGrpc.ClientServiceBlockingStub> nodeStubs = new HashMap<>();
    private final Map<String, AdminServiceGrpc.AdminServiceBlockingStub> adminStubs = new HashMap<>();
    private final pbft.client.ClientKeys clientKeys;
    private final KeyStore keyStore;
    private final int f;
    private final int n;
    private final long clientRpcTimeout;
    private final long clientBroadcastTimeout;
    private final long adminRpcTimeout;
    private final long clientBaseTimeout;
    private final long clientMaxTimeout;
    private final double backoffFactor;
    private final int maxAttempts;

    private final ConcurrentMap<String, ConcurrentMap<Long, ReplyGatherer>> collectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Long, Long>> expectedTs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Long, String>> opDescs = new ConcurrentHashMap<>();
    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final Set<String> failed = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService timers = Executors.newScheduledThreadPool(2, r -> { Thread t = new Thread(r, "bench-timers"); t.setDaemon(true); return t; });
    private final ConcurrentMap<String, Retransmit> retransmits = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, AtomicInteger> opTotals = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<String, AtomicInteger>> opByStatus = new ConcurrentHashMap<>();

    private Config shellCfg;

    public static final class Config {
        public final Path configPath;
        public final Path keysDir;
        public final long warmupSeconds;
        public final long durationSeconds;
        public final int transferPercent;
        public final long seed;
        public final double hotProb;
        public final int hotK;
        public final long maxOps;
        public Config(Path configPath, Path keysDir, long warmupSeconds, long durationSeconds, int transferPercent, long seed, double hotProb, int hotK, long maxOps) {
            this.configPath = configPath;
            this.keysDir = keysDir;
            this.warmupSeconds = warmupSeconds;
            this.durationSeconds = durationSeconds;
            this.transferPercent = transferPercent;
            this.seed = seed;
            this.hotProb = hotProb;
            this.hotK = hotK;
            this.maxOps = maxOps;
        }
    }

    public BenchmarkRunner(Path configPath, Path keysDir) throws IOException {
        Path absolutePath = configPath.toAbsolutePath();
        this.clusterConfig = ClusterConfig.load(absolutePath);
        Path baseDir = absolutePath.getParent() != null ? absolutePath.getParent().getParent() : null;
        if (baseDir == null) baseDir = absolutePath.getParent();
        if (baseDir == null) baseDir = Path.of(".");
        try { this.keyStore = KeyStore.from(clusterConfig, baseDir); } catch (GeneralSecurityException e) { throw new IOException("Failed to load replica key store: " + e.getMessage(), e); }
        this.clientKeys = pbft.client.ClientKeys.load(keysDir);
        this.f = clusterConfig.f;
        this.n = clusterConfig.n;
        for (var r : clusterConfig.replicas) {
            ManagedChannel ch = ManagedChannelBuilder.forAddress(r.host, r.port).usePlaintext().build();
            nodeChannels.put(r.id, ch);
            nodeStubs.put(r.id, ClientServiceGrpc.newBlockingStub(ch));
            adminStubs.put(r.id, AdminServiceGrpc.newBlockingStub(ch));
        }
        long rpc1 = Math.max(4000L, Long.getLong("pbft.client.rpc_deadline_ms", 8000L));
        long rpc2 = Math.max(4000L, Long.getLong("pbft.client.broadcast_deadline_ms", rpc1));
        long adm = Math.max(500L, Long.getLong("pbft.client.admin_deadline_ms", 2000L));
        this.clientRpcTimeout = rpc1;
        this.clientBroadcastTimeout = rpc2;
        this.adminRpcTimeout = adm;
        long base;
        try { String p = System.getProperty("pbft.client.base_ms"); base = (p != null) ? Long.parseLong(p) : 500L; } catch (Exception ignore) { base = 500L; }
        if (base < 100L) base = 100L;
        long max;
        try { String p = System.getProperty("pbft.client.max_ms"); max = (p != null) ? Long.parseLong(p) : base * 8; } catch (Exception ignore) { max = base * 8; }
        if (max < base) max = base;
        this.clientBaseTimeout = base;
        this.clientMaxTimeout = max;
        double bof;
        try { String p = System.getProperty("pbft.client.backoff_factor"); bof = (p != null) ? Double.parseDouble(p) : 2.0; } catch (Exception ignore) { bof = 2.0; }
        if (bof < 1.0) bof = 1.0;
        this.backoffFactor = bof;
        int att;
        try { String p = System.getProperty("pbft.client.max_attempts"); att = (p != null) ? Integer.parseInt(p) : 10; } catch (Exception ignore) { att = 10; }
        this.maxAttempts = Math.max(0, att);
    }

    private static List<Long> newSyncList() {
        return Collections.synchronizedList(new ArrayList<>());
    }


    public void run(Config cfg) throws Exception {
        if (!Boolean.getBoolean("pbft.benchmark.enabled")) throw new IllegalStateException("Benchmarking not enabled");
        resetAll();
        List<String> accounts = new ArrayList<>();
        for (char c = 'A'; c <= 'J'; c++) accounts.add(String.valueOf(c));
        Random rnd = new Random(cfg.seed);
        long warmupEnd = System.nanoTime() + Duration.ofSeconds(Math.max(0, cfg.warmupSeconds)).toNanos();
        long measureEnd = warmupEnd + Duration.ofSeconds(Math.max(1, cfg.durationSeconds)).toNanos();
        boolean smallbank = Boolean.getBoolean("pbft.benchmark.smallbank.enabled");
        System.out.println("BENCH CONFIG: smallbank=" + smallbank
                + (smallbank ? (" mix=" + java.util.Arrays.toString(smallBankMix()))
                             : (" xfer_pct=" + Math.max(0, Math.min(100, cfg.transferPercent))))
                + " hot_prob=" + String.format("%.2f", cfg.hotProb)
                + " hot_k=" + cfg.hotK
                + " warmup_s=" + cfg.warmupSeconds
                + " duration_s=" + cfg.durationSeconds);

        AtomicLong opsSent = new AtomicLong();
        AtomicLong opsDone = new AtomicLong();

        List<Long> latencyBal = newSyncList();
        List<Long> latencySP = newSyncList();
        List<Long> latencyAM = newSyncList();
        List<Long> latencyDC = newSyncList();
        List<Long> latencyAll = newSyncList();
        List<Long> latencyX = newSyncList();
        List<Long> latencyTS = newSyncList();
        List<Long> latencyWC = newSyncList();


        ExecutorService exec = Executors.newFixedThreadPool(10);
        List<Future<?>> futureArrayList = new ArrayList<>();
        Map<String, AtomicLong> tsByClient = new ConcurrentHashMap<>();
        for (String id : accounts) tsByClient.put(id, new AtomicLong(1));
        for (String clientId : accounts) {
            futureArrayList.add(exec.submit(() -> {
                try {
                while (System.nanoTime() < measureEnd) {
                    long t = tsByClient.get(clientId).getAndIncrement();
                    long start = System.nanoTime();
                    if (smallbank) {
                        int[] mix = smallBankMix();
                        int total = 0; for (int v : mix) total += v; if (total <= 0) total = 1;
                        int r = rnd.nextInt(total);
                        int idx = 0; int acc = mix[0]; while (r >= acc && idx < mix.length - 1) { idx++; acc += mix[idx]; }
                        int opIdx = idx;
                        int amtDC = (int)Math.max(1, Long.getLong("pbft.benchmark.smallbank.dc_amount", 1L));
                        int amtTS = (int)Math.max(1, Long.getLong("pbft.benchmark.smallbank.ts_amount", 1L));
                        int amtWC = (int)Math.max(1, Long.getLong("pbft.benchmark.smallbank.wc_amount", 1L));
                        int amtSP = (int)Math.max(1, Long.getLong("pbft.benchmark.smallbank.sp_amount", 1L));
                        String a = pickAccount(accounts, rnd, cfg.hotProb, cfg.hotK);
                        String b = pickDest(accounts, a, rnd);
                        switch (opIdx) {
                            case 0 -> { submitDepositChecking(clientId, t, a, amtDC); waitFor(clientId, t, clientRpcTimeout * 3); }
                            case 1 -> { submitTransactSavings(clientId, t, a, amtTS); waitFor(clientId, t, clientRpcTimeout * 3); }
                            case 2 -> { submitAmalgamate(clientId, t, a, b); waitFor(clientId, t, clientRpcTimeout * 3); }
                            case 3 -> { submitWriteCheck(clientId, t, a, amtWC); waitFor(clientId, t, clientRpcTimeout * 3); }
                            case 4 -> { submitSendPayment(clientId, t, a, b, amtSP); waitFor(clientId, t, clientRpcTimeout * 3); }
                            default -> { readOnlyBalance(clientId, t, a); waitFor(clientId, t, clientRpcTimeout * 3); }
                        }
                    } else {
                        boolean doTransfer = (rnd.nextInt(100) < cfg.transferPercent);
                        String a = pickAccount(accounts, rnd, cfg.hotProb, cfg.hotK);
                        String b = pickDest(accounts, a, rnd);
                        if (doTransfer) {
                            submitTransfer(clientId, t, a, b, 1);
                            waitFor(clientId, t, clientRpcTimeout * 3);
                        } else {
                            readOnlyBalance(clientId, t, a);
                            waitFor(clientId, t, clientRpcTimeout * 3);
                        }
                    }
                    long now = System.nanoTime();
                    boolean inWindow = (now >= warmupEnd && now < measureEnd);
                    boolean reached = completed.contains(clientId + "#" + t);
                    if (reached && inWindow) {
                        long dur = now - start;
                        latencyAll.add(dur);
                        if (smallbank) {
                            String desc = opDescs.getOrDefault(clientId, new ConcurrentHashMap<>()).getOrDefault(t, "");
                            if (desc.startsWith("DepositChecking")) latencyDC.add(dur);
                            else if (desc.startsWith("TransactSavings")) latencyTS.add(dur);
                            else if (desc.startsWith("Amalgamate")) latencyAM.add(dur);
                            else if (desc.startsWith("WriteCheck")) latencyWC.add(dur);
                            else if (desc.startsWith("SendPayment")) latencySP.add(dur);
                            else if (desc.startsWith("Balance")) latencyBal.add(dur);
                        } else {
                            if (opDescs.getOrDefault(clientId, new ConcurrentHashMap<>()).getOrDefault(t, "").startsWith("Transfer")) latencyX.add(dur); else latencyBal.add(dur);
                        }
                        opsDone.incrementAndGet();
                        if (cfg.maxOps > 0 && opsDone.get() >= cfg.maxOps) break;
                    }
                    opsSent.incrementAndGet();
                    if (cfg.maxOps > 0 && opsSent.get() >= cfg.maxOps && System.nanoTime() >= warmupEnd) break;
                }
                } catch (Exception e) { throw new RuntimeException(e); }
            }));
        }
        for (var ftr : futureArrayList) ftr.get();
        exec.shutdown();
        exec.awaitTermination(5, TimeUnit.SECONDS);
        long measuredNs = Math.max(1, measureEnd - warmupEnd);
        printReport(latencyAll, latencyX, latencyBal, measuredNs);
        if (smallbank) {
            if (!latencyDC.isEmpty()) printLatency("DEPOSIT_CHECKING", latencyDC);
            if (!latencyTS.isEmpty()) printLatency("TRANSACT_SAVINGS", latencyTS);
            if (!latencyAM.isEmpty()) printLatency("AMALGAMATE", latencyAM);
            if (!latencyWC.isEmpty()) printLatency("WRITE_CHECK", latencyWC);
            if (!latencySP.isEmpty()) printLatency("SEND_PAYMENT", latencySP);
        }

        if (!opTotals.isEmpty()) {
            System.out.println("OPS SUMMARY");
            java.util.ArrayList<String> types = new java.util.ArrayList<>(opTotals.keySet());
            types.sort(String::compareTo);
            for (String type : types) {
                int total = opTotals.getOrDefault(type, new java.util.concurrent.atomic.AtomicInteger(0)).get();
                var bySt = opByStatus.getOrDefault(type, new java.util.concurrent.ConcurrentHashMap<>());
                java.util.ArrayList<String> parts = new java.util.ArrayList<>();
                for (var e : bySt.entrySet()) {
                    parts.add(e.getKey() + "=" + e.getValue().get());
                }
                parts.sort(String::compareTo);
                System.out.println("  " + type + ": total=" + total + (parts.isEmpty() ? "" : " statuses=" + parts));
            }
        }
    }

    private String pickAccount(List<String> accts, Random rnd, double hotProb, int hotK) {
        if (hotK <= 0 || hotK >= accts.size()) return accts.get(rnd.nextInt(accts.size()));
        if (rnd.nextDouble() < hotProb) return accts.get(rnd.nextInt(hotK));
        return accts.get(hotK + rnd.nextInt(accts.size() - hotK));
    }

    private String pickDest(List<String> accts, String from, Random rnd) {
        String to;
        do { to = accts.get(rnd.nextInt(accts.size())); } while (to.equals(from));
        return to;
    }

    private void submitTransfer(String clientId, long reqSeq, String from, String to, long amount) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setTransfer(Transfer.newBuilder().setFrom(from).setTo(to).setAmount(amount).build()).build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "Transfer(" + from + "->" + to + ", amt=" + amount + ")");
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.submit(signedMessage);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage, false);
    }

    private void readOnlyBalance(String clientId, long reqSeq, String account) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setBalance(Balance.newBuilder().setAccount(account).build()).build())
                .build();
        SignedMessage env = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = 2 * f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasBalance() ? (r.getReqSeq() + "|" + Long.toString(r.getBalance())) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "Balance(" + account + ")");
        nodeStubs.entrySet().parallelStream().forEach(e -> {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.readOnly(env);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        });
        scheduleClientTimer(clientId, reqSeq, env, true);
    }

    private void waitFor(String clientId, long reqSeq, long maxWaitMs) {
        String key = clientId + "#" + reqSeq;
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        while (System.nanoTime() < deadline) {
            if (completed.contains(key) || failed.contains(key)) return;
            try { Thread.sleep(10); } catch (InterruptedException ignored) {}
        }
    }

    private void printReport(List<Long> latAll, List<Long> latXfer, List<Long> latBal, long measuredNs) {
        long cnt = latAll.size();
        double sec = measuredNs / 1_000_000_000.0;
        double tps = (sec > 0) ? cnt / sec : 0.0;
        System.out.println("BENCH: total=" + cnt + " time_sec=" + String.format("%.3f", sec) + " throughput_tps=" + String.format("%.2f", tps));
        if (!latAll.isEmpty()) printLatency("ALL", latAll);
        if (!latXfer.isEmpty()) printLatency("TRANSFER", latXfer);
        if (!latBal.isEmpty()) printLatency("BALANCE", latBal);
    }

    private void printLatency(String label, List<Long> lat) {
        long[] a = lat.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(a);
        double avg = 0.0;
        for (long v : a) avg += v;
        avg /= Math.max(1, a.length);
        long p50 = a[(int)Math.floor(0.50 * (a.length - 1))];
        long p95 = a[(int)Math.floor(0.95 * (a.length - 1))];
        long p99 = a[(int)Math.floor(0.99 * (a.length - 1))];
        System.out.println("LATENCY[" + label + "]: avg_ms=" + toMs(avg) + " p50_ms=" + toMs(p50) + " p95_ms=" + toMs(p95) + " p99_ms=" + toMs(p99));
    }

    private String toMs(double nanos) {
        return String.format("%.3f", nanos / 1_000_000.0);
    }

    private String toMs(long nanos) {
        return String.format("%.3f", nanos / 1_000_000.0);
    }

    private static final class ReplyGatherer {
        private final int threshold;
        private final KeyFn keyFn;
        private final ConcurrentMap<String, Set<String>> byResult = new ConcurrentHashMap<>();
        private final CountDownLatch latch = new CountDownLatch(1);
        private final java.util.concurrent.atomic.AtomicBoolean announced = new java.util.concurrent.atomic.AtomicBoolean(false);
        ReplyGatherer(int threshold, KeyFn keyFn) { this.threshold = threshold; this.keyFn = keyFn; }
        boolean addAndCheck(ClientReply r) {
            String k = keyFn.key(r);
            byResult.computeIfAbsent(k, _k -> ConcurrentHashMap.newKeySet()).add(r.getReplicaId());
            int size = byResult.get(k).size();
            if (size >= threshold && announced.compareAndSet(false, true)) { latch.countDown(); return true; }
            return false;
        }
        void await(long time, TimeUnit unit) { try { latch.await(time, unit); } catch (InterruptedException ignored) {} }
        boolean reached() { return announced.get(); }
        int threshold() { return threshold; }
    }

    private interface KeyFn { String key(ClientReply r); }

    private static final class Retransmit {
        volatile long delayMs;
        volatile ScheduledFuture<?> future;
        final java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean(false);
        volatile int attempts = 0;
        Retransmit(long d) { this.delayMs = d; }
    }

    private void loadCollectors(String clientId, long reqSeq, int threshold, KeyFn keyFn, long ts) {
        collectors.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, new ReplyGatherer(threshold, keyFn));
        expectedTs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, ts);
    }

    private void scheduleClientTimer(String clientId, long reqSeq, SignedMessage signedReq, boolean readOnly) {
        String key = clientId + "#" + reqSeq;
        Retransmit st = retransmits.computeIfAbsent(key, k -> new Retransmit(clientBaseTimeout));
        if (readOnly) scheduleReadOnlyTimeout(key, st, signedReq); else scheduleTimeout(key, st, signedReq);
    }

    private void scheduleTimeout(String key, Retransmit st, SignedMessage signedReq) {
        st.future = timers.schedule(() -> onClientTimeout(key, signedReq), st.delayMs, TimeUnit.MILLISECONDS);
    }

    private void scheduleReadOnlyTimeout(String key, Retransmit st, SignedMessage signedReq) {
        st.future = timers.schedule(() -> onClientReadOnlyTimeout(key, signedReq), st.delayMs, TimeUnit.MILLISECONDS);
    }

    private void onClientTimeout(String key, SignedMessage signedReq) {
        if (signedReq == null) return;
        Retransmit st = retransmits.get(key);
        if (st == null || st.cancelled.get()) return;
        String[] parts = key.split("#", 2);
        String clientId = parts[0];
        long reqSeq = Long.parseLong(parts[1]);
        if (completed.contains(key)) { cancelClientTimer(clientId, reqSeq); return; }
        var col = collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
        if (col != null && col.reached()) { cancelClientTimer(clientId, reqSeq); return; }
        if (maxAttempts > 0) {
            int a = st.attempts + 1;
            st.attempts = a;
            if (a >= maxAttempts) {
                failed.add(key);
                cancelClientTimer(clientId, reqSeq);
                return;
            }
        }
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientRpcTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.submit(signedReq);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        long nextDelay = (backoffFactor <= 1.0) ? clientBaseTimeout : Math.min((long)Math.ceil(st.delayMs * backoffFactor), clientMaxTimeout);
        st.delayMs = nextDelay;
        scheduleTimeout(key, st, signedReq);
    }

    private void onClientReadOnlyTimeout(String key, SignedMessage signedReq) {
        if (signedReq == null) return;
        Retransmit st = retransmits.get(key);
        if (st == null || st.cancelled.get()) return;
        String[] parts = key.split("#", 2);
        String clientId = parts[0];
        long reqSeq = Long.parseLong(parts[1]);
        if (completed.contains(key)) { cancelClientTimer(clientId, reqSeq); return; }
        var col = collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
        if (col != null && col.reached()) { cancelClientTimer(clientId, reqSeq); return; }
        if (maxAttempts > 0) {
            int a = st.attempts + 1;
            st.attempts = a;
            if (a >= maxAttempts) {
                failed.add(key);
                cancelClientTimer(clientId, reqSeq);
                return;
            }
        }
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientRpcTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.readOnly(signedReq);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        long nextDelay = (backoffFactor <= 1.0) ? clientBaseTimeout : Math.min((long)Math.ceil(st.delayMs * backoffFactor), clientMaxTimeout);
        st.delayMs = nextDelay;
        scheduleReadOnlyTimeout(key, st, signedReq);
    }

    private void cancelClientTimer(String clientId, long reqSeq) {
        String key = clientId + "#" + reqSeq;
        Retransmit st = retransmits.remove(key);
        if (st != null) {
            st.cancelled.set(true);
            var f = st.future;
            if (f != null) f.cancel(false);
        }
    }

    private void handleReplySignedMsg(SignedMessage replySigned) {
        if (replySigned == null) return;
        var tr = MessageDecoder.decodeClientReply(replySigned, keyStore);
        if (!tr.ok()) return;
        if (!tr.viewMatches()) return;
        ClientReply reply = tr.message();
        if (!tr.signerId().equals(reply.getReplicaId())) return;
        long rv = reply.getView();
        Long tsExp = expectedTs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).get(reply.getReqSeq());
        if (tsExp != null && reply.getReqSeq() != tsExp) return;
        var map = collectors.get(reply.getClientId());
        if (map != null) {
            var col = map.get(reply.getReqSeq());
            if (col != null) {
                boolean reached = col.addAndCheck(reply);
                if (reached) {
                    String ckey = reply.getClientId() + "#" + reply.getReqSeq();
                    if (!completed.add(ckey)) return;
                    try {
                        String desc = opDescs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).getOrDefault(reply.getReqSeq(), "");
                        String type = desc;
                        int l = desc.indexOf('(');
                        if (l > 0) type = desc.substring(0, l);
                        if (type.isEmpty() && reply.hasBalance()) type = "Balance";
                        if (type.isEmpty()) type = "Unknown";
                        opTotals.computeIfAbsent(type, _k -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
                        String status = reply.hasStatus() ? reply.getStatus() : (reply.hasBalance() ? "BAL" : "OK");
                        var stMap = opByStatus.computeIfAbsent(type, _k -> new java.util.concurrent.ConcurrentHashMap<>());
                        stMap.computeIfAbsent(status, _k -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
                    } catch (Exception ignored) {}
                    cancelClientTimer(reply.getClientId(), reply.getReqSeq());
                    collectors.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                    expectedTs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                    opDescs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                }
            }
        }
    }

    private int[] smallBankMix() {
        String s = System.getProperty("pbft.benchmark.smallbank.mix");
        if (s != null && !s.isEmpty()) {
            try {
                String[] parts = s.split(",");
                if (parts.length == 6) {
                    int dc = Integer.parseInt(parts[0].trim());
                    int ts = Integer.parseInt(parts[1].trim());
                    int am = Integer.parseInt(parts[2].trim());
                    int wc = Integer.parseInt(parts[3].trim());
                    int sp = Integer.parseInt(parts[4].trim());
                    int bal = Integer.parseInt(parts[5].trim());
                    return new int[]{dc, ts, am, wc, sp, bal};
                }
            } catch (Exception ignored) {}
        }
        return new int[]{1,1,1,1,1,1};
    }

    private void submitDepositChecking(String clientId, long reqSeq, String account, long amount) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setDepositChecking(DepositChecking.newBuilder().setAccount(account).setAmount(amount).build()).build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "DepositChecking(" + account + ", amt=" + amount + ")");
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.submit(signedMessage);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage, false);
    }

    private void submitTransactSavings(String clientId, long reqSeq, String account, long amount) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setTransactSavings(TransactSavings.newBuilder().setAccount(account).setAmount(amount).build()).build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "TransactSavings(" + account + ", amt=" + amount + ")");
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySignedMsg = stub.submit(signedMessage);
                handleReplySignedMsg(replySignedMsg);
            } catch (Exception ignored) {}
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage, false);
    }

    private void submitAmalgamate(String clientId, long reqSeq, String src, String dst) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setAmalgamate(Amalgamate.newBuilder().setSrc(src).setDst(dst).build()).build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "Amalgamate(" + src + "->" + dst + ")");
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.submit(signedMessage);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage, false);
    }

    private void submitWriteCheck(String clientId, long reqSeq, String account, long amount) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setWriteCheck(WriteCheck.newBuilder().setAccount(account).setAmount(amount).build()).build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "WriteCheck(" + account + ", amt=" + amount + ")");
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.submit(signedMessage);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage, false);
    }

    private void submitSendPayment(String clientId, long reqSeq, String src, String dst, long amount) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder().setSendPayment(SendPayment.newBuilder().setSrc(src).setDst(dst).setAmount(amount).build()).build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);
        opDescs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>()).put(reqSeq, "SendPayment(" + src + "->" + dst + ", amt=" + amount + ")");
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(clientBroadcastTimeout, TimeUnit.MILLISECONDS);
                SignedMessage replySigned = stub.submit(signedMessage);
                handleReplySignedMsg(replySigned);
            } catch (Exception ignored) {}
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage, false);
    }

    private AdminServiceGrpc.AdminServiceBlockingStub adminStubWithDeadline(AdminServiceGrpc.AdminServiceBlockingStub stub) {
        return stub.withDeadlineAfter(adminRpcTimeout, TimeUnit.MILLISECONDS);
    }

    private void resetAll() {
        retransmits.clear();
        collectors.clear();
        expectedTs.clear();
        opDescs.clear();
        completed.clear();
        failed.clear();
        opTotals.clear();
        opByStatus.clear();
        adminStubs.entrySet().parallelStream().forEach(e -> { try { adminStubWithDeadline(e.getValue()).reset(Empty.getDefaultInstance()); } catch (Exception ignored) {} });
        waitForResetOfCluster();
        setAllLiveNodes();
    }

    private void waitForResetOfCluster() {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(2000L, Long.getLong("pbft.client.reset_wait_ms", 8000L)));
        Set<String> pending = new HashSet<>();
        for (var r : clusterConfig.replicas) pending.add(r.id);
        while (System.nanoTime() < deadline && !pending.isEmpty()) {
            Iterator<String> it = pending.iterator();
            while (it.hasNext()) {
                String id = it.next();
                var stub = adminStubs.get(id);
                if (stub == null) { it.remove(); continue; }
                try {
                    PrintLogReply reply = adminStubWithDeadline(stub).printLog(Empty.getDefaultInstance());
                    if (reply.getEntriesCount() == 0) { it.remove(); } else { adminStubWithDeadline(stub).reset(Empty.getDefaultInstance()); }
                } catch (Exception ignored) {}
            }
            if (!pending.isEmpty()) { try { Thread.sleep(100); } catch (InterruptedException ignored) {} }
        }
    }

    private void setAllLiveNodes() {
        java.util.List<String> ids = new java.util.ArrayList<>();
        for (var r : clusterConfig.replicas) ids.add(r.id);
        LiveNodesRequest req = LiveNodesRequest.newBuilder().addAllNodeIds(ids).build();
        adminStubs.entrySet().parallelStream().forEach(e -> {
            try { adminStubWithDeadline(e.getValue()).setLiveNodes(req); } catch (Exception ignored) {}
        });
        try { Thread.sleep(100); } catch (InterruptedException ignored) {}
    }

    public void runInteractiveCLI(Config cfg) throws Exception {
        if (!Boolean.getBoolean("pbft.benchmark.enabled")) throw new IllegalStateException("Benchmarking not enabled");
        this.shellCfg = cfg;
        System.out.println("BENCHSHELL: type 'help' for commands.");
        ExecutorService driver = Executors.newSingleThreadExecutor(r -> { Thread t = new Thread(r, "bench-driver"); t.setDaemon(true); return t; });
        Future<?> future = null;
        java.util.Scanner sc = new java.util.Scanner(System.in);
        while (true) {
            if (future != null && future.isDone()) { System.out.println("Benchmark finished."); future = null; }
            System.out.print("bench> ");
            String line;
            try { line = sc.nextLine(); } catch (java.util.NoSuchElementException e) { break; }
            if (line == null) continue;
            line = line.trim();
            if (line.isEmpty()) continue;
            String[] tok = line.split("\\s+");
            String cmd = tok[0].toLowerCase();
            try {
                switch (cmd) {
                    case "help" -> {
                        System.out.println("Commands:");
                        System.out.println("  benchmark            - Run the benchmark");
                        System.out.println("  wait                 - Wait for benchmark run to complete");
                        System.out.println("  info                 - Show benchmark configuration");
                        System.out.println("  ops                  - Show operations");
                        System.out.println("  inprogress             - Show inprogress operations");
                        System.out.println("  db [nodeId]          - Print DB of all nodes or a specific node");
                        System.out.println("  log [nodeId]         - Print log of all nodes or a specific node");
                        System.out.println("  checkpoint [nodeId]  - Print checkpoint details of all nodes or a specific node");
                        System.out.println("  view [nodeId]        - Print view messages of all nodes or a specific node");
                        System.out.println("  quit                 - Exit benchmark");
                    }
                    case "benchmark" -> {
                        if (future != null) { System.out.println("Already running."); }
                        else { future = driver.submit(() -> { try { run(cfg); } catch (Exception e) { throw new RuntimeException(e); } }); }
                    }
                    case "wait" -> { if (future != null) { future.get(); future = null; } else { System.out.println("(no active run)"); } }
                    case "info" -> printInfo(cfg);
                    case "ops" -> printOpsSummary();
                    case "inprogress" -> printInProgress();
                    case "db" -> { if (tok.length >= 2) printDbOne(tok[1]); else printDbMatrixAll(); }
                    case "log" -> { if (tok.length >= 2) printLogOne(tok[1]); else printLogAll(); }
                    case "checkpoint" -> { if (tok.length >= 2) printCheckpointOne(tok[1]); else printCheckpointAll(); }
                    case "view" -> { if (tok.length >= 2) printViewOne(tok[1]); else printViewAll(); }
                    case "quit" -> { if (future != null) { System.out.println("Waiting for benchmark to finish..."); future.get(); } driver.shutdown(); return; }
                    default -> System.out.println("Unknown command. Type 'help'.");
                }
            } catch (Exception ex) {
                System.out.println("Error: " + ex.getMessage());
            }
        }
        if (future != null) { try { future.get(); } catch (Exception ignore) {} }
        driver.shutdown();
    }

    private void printInfo(Config cfg) {
        boolean smallbank = Boolean.getBoolean("pbft.benchmark.smallbank.enabled");
        System.out.println("INFO:");
        System.out.println("  n=" + n + " f=" + f);
        System.out.println("  smallbank=" + smallbank + (smallbank ? (" mix=" + java.util.Arrays.toString(smallBankMix())) : (" xfer_pct=" + cfg.transferPercent)));
        System.out.println("  hot_prob=" + String.format("%.2f", cfg.hotProb) + " hot_k=" + cfg.hotK);
        System.out.println("  warmup_s=" + cfg.warmupSeconds + " duration_s=" + cfg.durationSeconds);
        System.out.println("  timeouts: base_ms=" + clientBaseTimeout + " max_ms=" + clientMaxTimeout + " backoff=" + backoffFactor + " max_attempts=" + maxAttempts);
    }

    private void printOpsSummary() {
        if (opTotals.isEmpty()) { System.out.println("(no completed ops yet)"); return; }
        System.out.println("OPS SUMMARY (so far):");
        ArrayList<String> types = new ArrayList<>(opTotals.keySet());
        types.sort(String::compareTo);
        for (String type : types) {
            int total = opTotals.getOrDefault(type, new AtomicInteger(0)).get();
            var bySt = opByStatus.getOrDefault(type, new ConcurrentHashMap<>());
            ArrayList<String> parts = new ArrayList<>();
            for (var e : bySt.entrySet()) parts.add(e.getKey() + "=" + e.getValue().get());
            parts.sort(String::compareTo);
            System.out.println("  " + type + ": total=" + total + (parts.isEmpty() ? "" : " statuses=" + parts));
        }
    }

    private void printInProgress() {
        int inProgress = 0;
        ArrayList<String> sample = new ArrayList<>();
        for (var e : collectors.entrySet()) {
            String clientId = e.getKey();
            for (var kv : e.getValue().entrySet()) {
                var col = kv.getValue();
                if (col == null || col.reached()) continue;
                inProgress++;
                String desc = opDescs.getOrDefault(clientId, new ConcurrentHashMap<>()).getOrDefault(kv.getKey(), "");
                if (sample.size() < 20) sample.add(clientId + "#" + kv.getKey() + " " + desc + " thr=" + col.threshold());
            }
        }
        System.out.println("INPROGRESS: count=" + inProgress + (sample.isEmpty() ? "" : " sample=" + sample));
    }

    private void printDbAll() { for (var id : adminStubs.keySet()) { printDbOne(id); } }
    private void printLogAll() { for (var id : adminStubs.keySet()) { printLogOne(id); } }
    private void printCheckpointAll() { for (var id : adminStubs.keySet()) { printCheckpointOne(id); } }
    private void printViewAll() { for (var id : adminStubs.keySet()) { printViewOne(id); } }

    private void printDbMatrixAll() {
        java.util.ArrayList<String> nodes = new java.util.ArrayList<>(adminStubs.keySet());
        nodes.sort(String::compareTo);
        java.util.Map<String, java.util.Map<String, Long>> byNode = new java.util.HashMap<>();
        for (String id : nodes) {
            try {
                PrintDBReply r = adminStubWithDeadline(adminStubs.get(id)).printDB(Empty.getDefaultInstance());
                java.util.HashMap<String, Long> m = new java.util.HashMap<>();
                for (var b : r.getBalancesList()) m.put(b.getAccount(), b.getBalance());
                byNode.put(id, m);
            } catch (Exception e) {
                byNode.put(id, java.util.Collections.emptyMap());
            }
        }
        int accountWidth = Math.max("Account".length(), 1);
        java.util.Map<String, Integer> colWidths = new java.util.HashMap<>();
        for (String id : nodes) {
            int w = id.length();
            for (char c = 'A'; c <= 'J'; c++) {
                long v = byNode.getOrDefault(id, java.util.Collections.emptyMap()).getOrDefault(String.valueOf(c), 0L);
                int len = Long.toString(v).length();
                if (len > w) w = len;
            }
            colWidths.put(id, w);
        }
        StringBuilder horizontal = new StringBuilder();
        horizontal.append("+").append("-".repeat(accountWidth + 2)).append("+");
        for (String id : nodes) horizontal.append("-".repeat(colWidths.get(id) + 2)).append("+");
        StringBuilder header = new StringBuilder();
        header.append("| ").append(String.format("%-" + accountWidth + "s", "Account")).append(" |");
        for (String id : nodes) header.append(String.format(" %-" + colWidths.get(id) + "s |", id));
        System.out.println(horizontal);
        System.out.println(header);
        System.out.println(horizontal);
        for (char c = 'A'; c <= 'J'; c++) {
            String acct = String.valueOf(c);
            StringBuilder row = new StringBuilder();
            row.append("| ").append(String.format("%-" + accountWidth + "s", acct)).append(" |");
            for (String id : nodes) {
                long v = byNode.getOrDefault(id, java.util.Collections.emptyMap()).getOrDefault(acct, 0L);
                row.append(String.format(" %-" + colWidths.get(id) + "s |", Long.toString(v)));
            }
            System.out.println(row);
        }
        System.out.println(horizontal);
    }

    private void printDbOne(String nodeId) {
        var stub = adminStubs.get(nodeId);
        if (stub == null) { System.out.println("Unknown node: " + nodeId); return; }
        try {
            PrintDBReply r = adminStubWithDeadline(stub).printDB(Empty.getDefaultInstance());
            System.out.println("DB[" + nodeId +"]:");
            printBalanceTable(r.getBalancesList());
        } catch (Exception e) { System.out.println("DB[" + nodeId + "]: " + e); }
    }

    private void printLogOne(String nodeId) {
        var stub = adminStubs.get(nodeId);
        if (stub == null) { System.out.println("Unknown node: " + nodeId); return; }
        try {
            PrintLogReply r = adminStubWithDeadline(stub).printLog(Empty.getDefaultInstance());
            System.out.println("LOG[" + nodeId +"]:");
            for (var e : r.getEntriesList()) {
                String phases = String.format("PP=%s P=%s C=%s E=%s", e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                System.out.println("(v=" + e.getView() + ", s=" + e.getSeq() + ") client=" + e.getClientId() + "#" + e.getReqSeq() + " status=" + e.getStatus() + " digest=" + e.getRequestDigest() + " op=" + e.getOperation() + " phases[" + phases + "]");
            }
        } catch (Exception e) { System.out.println("LOG[" + nodeId + "]: " + e); }
    }

    private void printCheckpointOne(String nodeId) {
        var stub = adminStubs.get(nodeId);
        if (stub == null) { System.out.println("Unknown node: " + nodeId); return; }
        try {
            PrintCheckpointReply r = adminStubWithDeadline(stub).printCheckpoint(Empty.getDefaultInstance());
            String dg = Hex.toHexOrEmpty(r.getLastStableDigest().toByteArray());
            if (dg.length() > 8) dg = dg.substring(0, 8);
            System.out.println("CHECKPOINT[" + nodeId +"]:");
            System.out.println("enabled=" + r.getEnabled());
            System.out.println("period=" + r.getPeriod());
            System.out.println("last_stable_seq=" + r.getLastStableSeq());
            System.out.println("last_stable_digest=" + dg);
            System.out.println("stable_signers=" + r.getStableSigners());
            if (r.getStableSignerIdsCount() > 0) System.out.println("stable_signer_ids=" + r.getStableSignerIdsList());
            System.out.println("has_local_snapshot=" + r.getHasLocalSnapshot());
            try {
                var entries = r.getCheckpointEntriesList();
                if (entries != null && !entries.isEmpty()) {
                    System.out.println("window_entries (" + entries.size() + "):");
                    for (var e : entries) {
                        String phases = String.format("PP=%s P=%s C=%s E=%s", e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                        System.out.println("  (v=" + e.getView() + ", s=" + e.getSeq() + ") client=" + e.getClientId() + "#" + e.getReqSeq() + " op=" + e.getOperation() + " phases[" + phases + "]");
                    }
                }
            } catch (Throwable ignore) {}
        } catch (Exception e) { System.out.println("CHECKPOINT[" + nodeId + "]: " + e); }
    }

    private void printViewOne(String nodeId) {
        var stub = adminStubs.get(nodeId);
        if (stub == null) { System.out.println("Unknown node: " + nodeId); return; }
        try {
            ViewLog r = adminStubWithDeadline(stub).printView(Empty.getDefaultInstance());
            System.out.println("VIEW[" + nodeId +"]:");
            for (var s : r.getNewViewsList()) System.out.println(s);
            if (r.getHasLast()) {
                System.out.println("Last NewView v'=" + r.getLastView() + " s=" + r.getS() + " h=" + r.getH());
                printNewView(r.getLastNewView(), "  ");
            }
        } catch (Exception e) { System.out.println("VIEW[" + nodeId + "]: " + e); }
    }

    private void printNewView(NewView nv, String indent) {
        if (nv.getSelectedPrepreparesCount() > 0) {
            System.out.println(indent + "Selected PrePrepares:");
            for (var pp : nv.getSelectedPrepreparesList()) {
                String dg = Hex.toHexOrEmpty(pp.getRequestDigest().toByteArray());
                if (dg.length() > 8) dg = dg.substring(0, 8);
                boolean hasReq = pp.hasClientRequest();
                System.out.println(indent + "  (s=" + pp.getSeq() + ", v=" + pp.getView() + ", dig=" + dg + ", client=" + pp.getClientId() + "#" + pp.getReqSeq() + ", has_req=" + hasReq + ")");
            }
        }
        if (nv.getViewChangesCount() > 0) {
            System.out.println(indent + "ViewChanges (" + nv.getViewChangesCount() + "):");
            for (var env : nv.getViewChangesList()) {
                var vctr = MessageDecoder.decodeViewChange(env, keyStore);
                if (vctr.ok()) {
                    var vc = vctr.message();
                    System.out.println(indent + "  signer=" + vctr.signerId() + " v'=" + vc.getNewView() + " s=" + vc.getLastStableSeq() + " Pset=" + vc.getPsetCount());
                    for (var pe : vc.getPsetList()) {
                        String dg = Hex.toHexOrEmpty(pe.getRequestDigest().toByteArray());
                        if (dg.length() > 8) dg = dg.substring(0, 8);
                        System.out.println(indent + "    pe(s=" + pe.getSeq() + ", v=" + pe.getView() + ", dig=" + dg + ", client=" + pe.getClientId() + "#" + pe.getReqSeq() + ", proof=" + pe.hasProof() + ")");
                    }
                } else {
                    System.out.println(indent + "  [invalid VC envelope]");
                }
            }
        }
    }

    private static void printBalanceTable(java.util.List<BalanceEntry> balances) {
        if (balances.isEmpty()) { System.out.println("(empty)"); return; }
        int accountWidth = Math.max("Account".length(), balances.stream().map(b -> b.getAccount().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int balanceWidth = Math.max("Balance".length(), balances.stream().map(b -> Long.toString(b.getBalance()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        String horizontal = "+" + "-".repeat(accountWidth + 2) + "+" + "-".repeat(balanceWidth + 2) + "+";
        String header = String.format("| %-" + accountWidth + "s | %-" + balanceWidth + "s |", "Account", "Balance");
        System.out.println(horizontal);
        System.out.println(header);
        System.out.println(horizontal);
        for (var b : balances) {
            String row = String.format("| %-" + accountWidth + "s | %-" + balanceWidth + "s |", b.getAccount(), Long.toString(b.getBalance()));
            System.out.println(row);
        }
        System.out.println(horizontal);
    }

    @Override
    public void close() {
        try { timers.shutdownNow(); } catch (Exception ignored) {}
        for (var ch : nodeChannels.values()) { try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {} }
    }
}
