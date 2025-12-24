package pbft.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import pbft.common.config.ClusterConfig;
import pbft.common.config.KeyStore;
import pbft.common.crypto.RequestType;
import pbft.common.validation.MessageDecoder;
import pbft.common.validation.SignedMessagePacker;
import pbft.common.validation.PbftMsgTypes;
import pbft.proto.*;
import pbft.common.util.Hex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public final class ClientController implements AutoCloseable {
    private static final boolean READ_ONLY_AS_READ_WRITE = Boolean.parseBoolean(System.getProperty("pbft.client.readonly.fallback", "true"));
    private static final long CLIENT_RPC_DEADLINE_MS = Math.max(4000L, Long.getLong("pbft.client.rpc_deadline_ms", 8000L));
    private static final long CLIENT_BROADCAST_DEADLINE_MS = Math.max(4000L, Long.getLong("pbft.client.broadcast_deadline_ms", CLIENT_RPC_DEADLINE_MS));
    private static final long ADMIN_RPC_DEADLINE_MS = Math.max(500L, Long.getLong("pbft.client.admin_deadline_ms", 2000L));
    private static final long RESET_WAIT_MS = Math.max(2000L, Long.getLong("pbft.client.reset_wait_ms", 8000L));
    private static final String RESET_MODE = System.getProperty("pbft.client.reset_mode", "all");

    private final ClusterConfig clusterConfig;
    private final Map<String, ManagedChannel> nodeChannels = new HashMap<>();
    private final Map<String, ClientServiceGrpc.ClientServiceBlockingStub> nodeStubs = new HashMap<>();
    private final Map<String, AdminServiceGrpc.AdminServiceBlockingStub> adminStubs = new HashMap<>();
    private final ClientKeys clientKeys;
    private final KeyStore keyStore;
    private final int f;
    private final int n;
    private volatile String leaderId;
    private final AtomicLong leaderView = new AtomicLong(1);
    private final String host;
    private final int port;
    private Server server;

    private final ConcurrentMap<String, ConcurrentMap<Long, ReplyGatherer>> collectors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Long, Long>> expectedTs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<Long, String>> operationDescriptions = new ConcurrentHashMap<>();
    private final java.util.Set<String> readOnlyAsReadWriteSet = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final Set<String> failed = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean abortSet = new AtomicBoolean(false);

    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "client-timers"); t.setDaemon(true); return t; });
    private final ConcurrentMap<String, Retransmit> retransmits = new ConcurrentHashMap<>();
    private final long clientBaseTimeout;
    private final long clientMaxTimeout;
    private final double backoffFactor;
    private final int maxAttempts;
    private final boolean stopSetOnNoQuorum;
    private final boolean stopAfterFirstFailureWhenNoQuorum;
    private volatile boolean quorumPossible = true;

    private final AtomicBoolean cliSkip = new AtomicBoolean(false);
    private volatile Integer cliGoto = null;

    public ClientController(Path configPath, Path keysDir) throws IOException {
        Path absolutePath = configPath.toAbsolutePath();
        this.clusterConfig = ClusterConfig.load(absolutePath);
        Path baseDir = absolutePath.getParent() != null ? absolutePath.getParent().getParent() : null;

        if (baseDir == null) {
            baseDir = absolutePath.getParent();
        }
        try {
            this.keyStore = KeyStore.from(clusterConfig, baseDir);
        }
        catch (GeneralSecurityException e) {
            throw new IOException("Failed to load replica key store: " + e.getMessage(), e);
        }


        this.clientKeys = ClientKeys.load(keysDir);
        this.f = clusterConfig.f;
        this.n = clusterConfig.n;
        this.host = clusterConfig.clients.get(0).host;
        this.port = clusterConfig.clients.get(0).port;
        this.server = NettyServerBuilder.forAddress(new InetSocketAddress(host, port))
                .addService(new ClientReplyServiceImpl())
                .build()
                .start();


        clusterConfig.replicas.forEach(replica -> {
            ManagedChannel ch = ManagedChannelBuilder.forAddress(replica.host, replica.port).usePlaintext().build();
            nodeChannels.put(replica.id, ch);
            nodeStubs.put(replica.id, ClientServiceGrpc.newBlockingStub(ch));
            adminStubs.put(replica.id, AdminServiceGrpc.newBlockingStub(ch));
        });
        this.leaderId = primaryId(1);

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
        this.stopSetOnNoQuorum = Boolean.parseBoolean(System.getProperty("pbft.client.abort_set_on_quorum_fail", "true"));
        this.stopAfterFirstFailureWhenNoQuorum = Boolean.parseBoolean(System.getProperty("pbft.client.stop_on_first_failure", "true"));
    }

    public void runCsv(Path csvPath) throws Exception {
        Map<Integer, TestSet> sets = parseCsv(csvPath);

        System.out.println("Loaded sets: " + new TreeSet<>(sets.keySet()));

        ArrayList<Integer> order = new java.util.ArrayList<>(sets.keySet());
        Collections.sort(order);
        boolean autoRunNext = false;
        try (var in = new BufferedReader(new InputStreamReader(System.in))) {
            outer:
            for (int idx = 0; idx < order.size(); idx++) {
                int setNum = order.get(idx);
                var spec = sets.get(setNum);
                if (!autoRunNext) {
                    System.out.println("Ready to run set " + setNum + " (" + spec.txs.size() + " transactions). Press 'run' to start or type 'help' for commands.");
                    for(;;) {
                        System.out.print("> "); System.out.flush();
                        String line = in.readLine();
                        if (line == null) return;
                        line = line.trim();
                        if ("run".equalsIgnoreCase(line)) {
                            break;
                        }
                        if (!line.isEmpty()) {
                            if (handleCli(line)) return;
                            if (cliSkip.getAndSet(false)) { System.out.println("Skipping set " + setNum + "."); continue outer; }
                            Integer tg = this.cliGoto; if (tg != null) {
                                this.cliGoto = null;
                                int pos = java.util.Collections.binarySearch(order, tg);
                                if (pos >= 0) { System.out.println("Jumping to set " + tg + "."); idx = pos - 1; continue outer; }
                                else { System.out.println("Unknown set: " + tg); }
                            }
                            continue;
                        }
                    }
                } else {
                    autoRunNext = false;
                }

                resetAll();
                applyLiveNodes(spec.liveNodes);
                applyAttackConfig(spec);
                awaitClusterReady(spec.liveNodes);
                warmupLeader(spec.liveNodes);
                runSet(setNum, spec.txs);

                if (true) {
                    System.out.println("Set " + setNum + " is completed. Press 'run' to continue to next set or type 'help' for commands.");
                    for(;;) {
                        System.out.print("> "); System.out.flush();
                        String line = in.readLine();
                        if (line == null) return;
                        line = line.trim();
                        if ("run".equalsIgnoreCase(line)) { autoRunNext = true; break; }
                        if (!line.isEmpty()) {
                            if (handleCli(line)) return;
                            if (cliSkip.getAndSet(false)) { System.out.println("Skipping to next set."); break; }
                            Integer tg = this.cliGoto; if (tg != null) {
                                this.cliGoto = null;
                                int pos = java.util.Collections.binarySearch(order, tg);
                                if (pos >= 0) { System.out.println("Jumping to set " + tg + "."); idx = pos - 1; break; }
                                else { System.out.println("Unknown set: " + tg); }
                            }
                            continue;
                        }
                    }
                }
            }
            System.out.println("All sets completed. Press Enter to exit, or type commands ('help').");
            for(;;) {
                System.out.print("> "); System.out.flush();
                String line = in.readLine();
                if (line == null) return;
                line = line.trim();
                if (!line.isEmpty()) { if (handleCli(line)) return; continue; }
                break;
            }
        }
    }


    private void runSet(int setNum, List<String> txs) throws Exception {
        System.out.println("Running set " + setNum + ": " + txs.size() + " transactions");

        Map<String, BlockingQueue<Txn>> clientBlockingQueueMap = new HashMap<>();
        ConcurrentHashMap<String, AtomicLong> clientRequestTimeStamp = new ConcurrentHashMap<>();
        java.util.Set<String> pendingRequests = java.util.concurrent.ConcurrentHashMap.newKeySet();

        for (char c = 'A'; c <= 'J'; c++) {
            clientBlockingQueueMap.put(String.valueOf(c), new LinkedBlockingQueue<>());
            clientRequestTimeStamp.put(String.valueOf(c), new AtomicLong(1));
        }

        for (String line : txs) {
            line = line.trim();
            if (line.isEmpty()) continue;
            Txn t = parseTxn(line);
            if (t == null) continue;
            clientBlockingQueueMap.get(t.clientId()).add(t);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<?>> futures = new ArrayList<>();
        for (char c = 'A'; c <= 'J'; c++) {
            String clientId = String.valueOf(c);
            futures.add(executorService.submit(() -> {
                try {
                    while (true) {
                        if (stopSetOnNoQuorum && abortSet.get()) break;
                        Txn t = clientBlockingQueueMap.get(clientId).poll(100, TimeUnit.MILLISECONDS);
                        if (t == null) {
                            if (allEmpty(clientBlockingQueueMap)) break;
                            else continue;
                        }
                        long requestTimestamp = clientRequestTimeStamp.get(clientId).getAndIncrement();
                        pendingRequests.add(reqKey(clientId, requestTimestamp));
                        if (t.type == Txn.Type.TRANSFER) {
                            submitTransfer(clientId, requestTimestamp, t.from, t.to, t.amount);
                        }
                        else if (t.type == Txn.Type.BALANCE) {
                            readOnlyBalance(clientId, requestTimestamp, t.from);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        for (var ftr : futures) ftr.get();
        executorService.shutdown();
        executorService.awaitTermination(3, TimeUnit.SECONDS);
        waitForPending(pendingRequests);
    }

    private boolean allEmpty(Map<String, BlockingQueue<Txn>> q) {
        for (var e : q.values()) if (!e.isEmpty()) return false;
        return true;
    }

    private void submitTransfer(String clientId, long reqSeq, String from, String to, long amount) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder()
                        .setTransfer(Transfer.newBuilder().setFrom(from).setTo(to).setAmount(amount).build())
                        .build())
                .build();
        SignedMessage signedMessage = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));

        int threshold = f + 1;

        loadCollectors(clientId, reqSeq, threshold, r -> r.hasStatus() ? (r.getReqSeq() + "|" + r.getStatus()) : "", reqSeq);

        operationDescriptions.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>())
                .put(reqSeq, "Transfer(" + from + "->" + to + ", amt=" + amount + ")");

        ClientServiceGrpc.ClientServiceBlockingStub leaderStub = nodeStubs.get(leaderId);
        if (leaderStub == null && !nodeStubs.isEmpty()) leaderStub = nodeStubs.values().iterator().next();

        boolean sentToLeader = false;
        if (leaderStub != null) {
            try {
            SignedMessage signedMsg = leaderStub.withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS).submit(signedMessage);
                handleReplySignedMsg(signedMsg);
                sentToLeader = true;
            } catch (Exception ex) {
            }
        }

        if (!sentToLeader) {
            for (var e : nodeStubs.entrySet()) {
                try {
                    var stub = e.getValue().withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                    SignedMessage replyEnv = stub.submit(signedMessage);
                    handleReplySignedMsg(replyEnv);
                } catch (Exception ignored) {}
            }
        }
        scheduleClientTimer(clientId, reqSeq, signedMessage);
        String key = reqKey(clientId, reqSeq);
        while (!completed.contains(key) && !failed.contains(key)) {
            if (stopSetOnNoQuorum && abortSet.get()) break;
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
        }
        if (!completed.contains(key)) {
            cancelClientTimer(clientId, reqSeq);
            collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).remove(reqSeq);
            expectedTs.getOrDefault(clientId, new ConcurrentHashMap<>()).remove(reqSeq);
            operationDescriptions.getOrDefault(clientId, new ConcurrentHashMap<>()).remove(reqSeq);
        }
    }

    private void readOnlyBalance(String clientId, long reqSeq, String account) throws GeneralSecurityException {
        ClientRequest req = ClientRequest.newBuilder()
                .setClientId(clientId)
                .setReqSeq(reqSeq)
                .setOperation(Operation.newBuilder()
                        .setBalance(Balance.newBuilder().setAccount(account).build())
                        .build())
                .build();
        SignedMessage env = SignedMessagePacker.encode(RequestType.CLIENT_REQUEST, PbftMsgTypes.CLIENT_REQUEST, req, clientId, 0, 0, clientKeys.get(clientId));
        int threshold = 2 * f + 1;
        loadCollectors(clientId, reqSeq, threshold, r -> r.hasBalance() ? (r.getReqSeq() + "|" + Long.toString(r.getBalance())) : "", reqSeq);
        operationDescriptions.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>())
                .put(reqSeq, "Balance(" + account + ")");
        nodeStubs.entrySet().parallelStream().forEach(e -> {
            try {
                var stub = e.getValue().withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                SignedMessage replyEnv = stub.readOnly(env);
                handleReplySignedMsg(replyEnv);
            } catch (Exception ignored) {}
        });
        scheduleReadOnlyTimer(clientId, reqSeq, env);
        String keyRo = reqKey(clientId, reqSeq);
        while (!completed.contains(keyRo) && !failed.contains(keyRo)) {
            if (stopSetOnNoQuorum && abortSet.get()) break;
            try { Thread.sleep(20); } catch (InterruptedException ignored) {}
        }
        if (!completed.contains(keyRo)) {
        }
        collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).remove(reqSeq);
        expectedTs.getOrDefault(clientId, new ConcurrentHashMap<>()).remove(reqSeq);
        operationDescriptions.getOrDefault(clientId, new ConcurrentHashMap<>()).remove(reqSeq);
    }

    private interface KeyFn { String key(ClientReply r); }

    private void loadCollectors(String clientId, long reqSeq, int threshold, KeyFn keyFn, long ts) {
        collectors.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>())
                .put(reqSeq, new ReplyGatherer(threshold, keyFn));
        expectedTs.computeIfAbsent(clientId, _k -> new ConcurrentHashMap<>())
                .put(reqSeq, ts);
    }


    private void awaitReplies(String clientId, long reqSeq, long time, TimeUnit unit) {
        var col = collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
        if (col != null) col.await(time, unit);
    }

    private boolean awaitUntilReached(String clientId, long reqSeq, long maxWaitMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        while (System.nanoTime() < deadline) {
            var col = collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
            if (col != null && col.reached()) return true;
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
        }
        var col = collectors.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
        return col != null && col.reached();
    }

    private void handleReplySignedMsg(SignedMessage signedMessage) {
        if (signedMessage == null) {
            return;
        }
        var tr = MessageDecoder.decodeClientReply(signedMessage, keyStore);
        if (!tr.ok()) {
            return;
        }
        if (!tr.viewMatches()) {
            System.err.println("Ignoring ClientReply with mismatched signed message header from " + tr.signerId());
            return;
        }
        ClientReply reply = tr.message();
        if (!tr.signerId().equals(reply.getReplicaId())) {
            System.err.println("Ignoring ClientReply with signer/replica mismatch from " + tr.signerId());
            return;
        }
        long rv = reply.getView();
        while (true) {
            long cur = leaderView.get();
            if (rv <= cur) break;
            if (leaderView.compareAndSet(cur, rv)) { leaderId = primaryId(rv); break; }
        }
        Long tsExp = expectedTs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).get(reply.getReqSeq());
        if (tsExp != null && reply.getReqSeq() != tsExp) {
            return;
        }
        var map = collectors.get(reply.getClientId());
        if (map != null) {
            var col = map.get(reply.getReqSeq());
            if (col != null) {
                boolean reached = col.addAndCheck(reply);
                if (reached) {
                    String ckey = reply.getClientId() + "#" + reply.getReqSeq();
                    if (!completed.add(ckey)) {
                        return;
                    }
                    cancelClientTimer(reply.getClientId(), reply.getReqSeq());
                    String desc = operationDescriptions.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).get(reply.getReqSeq());
                    boolean quiet = Boolean.getBoolean("pbft.client.quiet");
                    if (reply.hasBalance()) {
                        if (!quiet) {
                            System.out.println(
                                    (desc != null ? desc : ("Balance(" + reply.getClientId() + ")")) +
                                            " = " + reply.getBalance() +
                                            " t=" + reply.getReqSeq() +
                                            " (reached " + col.threshold() + " matching replies)"
                            );
                        }
                        collectors.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                        expectedTs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                        operationDescriptions.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                        readOnlyAsReadWriteSet.remove(ckey);
                    } else if (reply.hasStatus()) {
                        if (!quiet) {
                            System.out.println(
                                    (desc != null ? desc : ("Transfer(" + reply.getClientId() + "#" + reply.getReqSeq() + ")")) +
                                            " = " + reply.getStatus() +
                                            " t=" + reply.getReqSeq() +
                                            " (reached " + col.threshold() + " matching replies)"
                            );
                        }
                        collectors.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                        expectedTs.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                        operationDescriptions.getOrDefault(reply.getClientId(), new ConcurrentHashMap<>()).remove(reply.getReqSeq());
                    }
                }
            }
        }
    }

    private final class ClientReplyServiceImpl extends ClientReplyServiceGrpc.ClientReplyServiceImplBase {
        @Override
        public void deliver(SignedMessage replyEnv, io.grpc.stub.StreamObserver<Empty> resp) {
            handleReplySignedMsg(replyEnv);
            resp.onNext(Empty.getDefaultInstance()); resp.onCompleted();
        }
    }
    private static final class Retransmit {
        volatile long delayMs;
        volatile ScheduledFuture<?> future;
        final java.util.concurrent.atomic.AtomicBoolean cancelled = new java.util.concurrent.atomic.AtomicBoolean(false);
        volatile int attempts = 0;
        Retransmit(long d) { this.delayMs = d; }
    }

    private String reqKey(String clientId, long reqSeq) { return clientId + "#" + reqSeq; }

    private void scheduleClientTimer(String clientId, long reqSeq, SignedMessage signedReq) {
        String key = reqKey(clientId, reqSeq);
        Retransmit st = retransmits.computeIfAbsent(key, k -> new Retransmit(clientBaseTimeout));
        scheduleTimeout(key, st, signedReq);
    }

    private void scheduleTimeout(String key, Retransmit st, SignedMessage signedReq) {
        st.future = timers.schedule(() -> onClientTimeout(key, signedReq), st.delayMs, TimeUnit.MILLISECONDS);
    }

    private void scheduleReadOnlyTimer(String clientId, long reqSeq, SignedMessage signedReq) {
        if (maxAttempts <= 0) return;
        String key = reqKey(clientId, reqSeq);
        Retransmit st = retransmits.computeIfAbsent(key, k -> new Retransmit(clientBaseTimeout));
        scheduleReadOnlyTimeout(key, st, signedReq);
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
                String desc = operationDescriptions.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
                if (desc == null) desc = key;
                System.out.println("No quorum reached for " + desc + " after " + a + " attempt(s).");
                failed.add(key);
                cancelClientTimer(clientId, reqSeq);
                if (stopSetOnNoQuorum && stopAfterFirstFailureWhenNoQuorum && !quorumPossible && completed.isEmpty()) {
                    abortSet.set(true);
                }
                return;
            }
        }
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                SignedMessage replyEnv = stub.submit(signedReq);
                handleReplySignedMsg(replyEnv);
            } catch (Exception ignored) {}
        }
        long nextDelay = (backoffFactor <= 1.0)
                ? clientBaseTimeout
                : Math.min((long)Math.ceil(st.delayMs * backoffFactor), clientMaxTimeout);
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
                String desc = operationDescriptions.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
                if (desc == null) desc = key;
                System.out.println("No quorum reached for " + desc + " after " + a + " attempt(s).");
                failed.add(key);
                cancelClientTimer(clientId, reqSeq);
                if (stopSetOnNoQuorum && stopAfterFirstFailureWhenNoQuorum && !quorumPossible && completed.isEmpty()) {
                    abortSet.set(true);
                }
                return;
            }
        }
        if (READ_ONLY_AS_READ_WRITE && !readOnlyAsReadWriteSet.contains(key)) {
            readOnlyAsReadWriteSet.add(key);
            int threshold = f + 1;
            loadCollectors(clientId, reqSeq, threshold, r -> r.hasBalance() ? (r.getReqSeq() + "|" + Long.toString(r.getBalance())) : "", reqSeq);
            ClientServiceGrpc.ClientServiceBlockingStub leaderStub = nodeStubs.get(leaderId);
            if (leaderStub == null && !nodeStubs.isEmpty()) leaderStub = nodeStubs.values().iterator().next();
            boolean sentToLeader = false;
            if (leaderStub != null) {
                try {
                    SignedMessage replyEnv = leaderStub.withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS).submit(signedReq);
                    handleReplySignedMsg(replyEnv);
                    sentToLeader = true;
                } catch (Exception ignored) {}
            }
            cancelClientTimer(clientId, reqSeq);
            scheduleClientTimer(clientId, reqSeq, signedReq);
            return;
        }
        for (var e : nodeStubs.entrySet()) {
            try {
                var stub = e.getValue().withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
                SignedMessage replyEnv = stub.readOnly(signedReq);
                handleReplySignedMsg(replyEnv);
            } catch (Exception ignored) {}
        }
        long nextDelay = (backoffFactor <= 1.0)
                ? clientBaseTimeout
                : Math.min((long)Math.ceil(st.delayMs * backoffFactor), clientMaxTimeout);
        st.delayMs = nextDelay;
        scheduleReadOnlyTimeout(key, st, signedReq);
    }

    private void cancelClientTimer(String clientId, long reqSeq) {
        String key = reqKey(clientId, reqSeq);
        Retransmit st = retransmits.remove(key);
        if (st != null) {
            st.cancelled.set(true);
            var f = st.future;
            if (f != null) f.cancel(false);
        }
    }

    private void waitForPending(java.util.Set<String> pendingRequests) throws InterruptedException {
        if (pendingRequests.isEmpty()) {
            return;
        }
        long baseWaitMs = Math.max(Math.max(clientMaxTimeout, CLIENT_RPC_DEADLINE_MS), clientBaseTimeout);
        long maxWaitMs = Math.max(15000L, baseWaitMs * 3);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMs);
        int lastOutstanding = -1;
        boolean announced = false;
        boolean suppressProgress = stopSetOnNoQuorum && stopAfterFirstFailureWhenNoQuorum;
        boolean aborted = false;
        while (System.nanoTime() < deadline) {
            int outstanding = 0;
            for (String key : pendingRequests) {
                if (!completed.contains(key) && !failed.contains(key)) outstanding++;
            }
            if (stopSetOnNoQuorum && abortSet.get()) {
                aborted = true;
                break;
            }
            if (outstanding == 0) {
                return;
            }
            if (!suppressProgress) {
                if (!announced) {
                    System.out.println("Waiting for " + outstanding + " request(s) to reach quorum (view-change or retransmission in progress)...");
                    announced = true;
                } else if (outstanding != lastOutstanding) {
                    System.out.println("  outstanding requests remaining: " + outstanding);
                }
            }
            lastOutstanding = outstanding;
            Thread.sleep(200);
        }
        if (aborted) {
            System.out.println("Quorum can't be reached after max attempts. Aborting set.");
            return;
        }
        List<String> stillPending = new ArrayList<>();
        List<String> failedList = new ArrayList<>();
        for (String key : pendingRequests) {
            if (failed.contains(key)) failedList.add(key);
            else if (!completed.contains(key)) stillPending.add(key);
        }
        if (!failedList.isEmpty()) {
            System.out.println("Quorum not reached for " + failedList.size() + " request(s):");
            for (String key : failedList) {
                String[] parts = key.split("#", 2);
                String clientId = parts[0];
                long reqSeq = Long.parseLong(parts[1]);
                String desc = operationDescriptions.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
                if (desc == null) desc = key;
                System.out.println("  FAILED " + key + " -> " + desc);
            }
        }
        if (!stillPending.isEmpty()) {
            System.out.println("Warning: " + stillPending.size() + " request(s) still pending after " + maxWaitMs + "ms:");
            for (String key : stillPending) {
                String[] parts = key.split("#", 2);
                String clientId = parts[0];
                long reqSeq = Long.parseLong(parts[1]);
                String desc = operationDescriptions.getOrDefault(clientId, new ConcurrentHashMap<>()).get(reqSeq);
                if (desc == null) desc = key;
                System.out.println("  PENDING " + key + " -> " + desc);
            }
        }
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
            if (size >= threshold && announced.compareAndSet(false, true)) {
                latch.countDown();
                return true;
            }
            return false;
        }

        void await(long time, TimeUnit unit) {
            try { latch.await(time, unit); } catch (InterruptedException ignored) {}
        }

        boolean reached() { return announced.get(); }
        int threshold() { return threshold; }
    }

    private AdminServiceGrpc.AdminServiceBlockingStub adminStubWithDeadline(AdminServiceGrpc.AdminServiceBlockingStub stub) {
        return stub.withDeadlineAfter(ADMIN_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
    }

    private void resetAll() {
        try {
            for (var e : retransmits.entrySet()) {
                try { cancelClientTimer(e.getKey().split("#",2)[0], Long.parseLong(e.getKey().split("#",2)[1])); } catch (Exception ignored) {}
            }
        } finally {
            retransmits.clear();
        }
        collectors.clear();
        expectedTs.clear();
        operationDescriptions.clear();
        completed.clear();
        failed.clear();
        abortSet.set(false);
        readOnlyAsReadWriteSet.clear();

        adminStubs.entrySet().parallelStream().forEach(e -> {
            try {
                adminStubWithDeadline(e.getValue()).reset(Empty.getDefaultInstance());
            } catch (Exception ignored) {}
        });
        awaitClusterReset();
    }

    private void awaitClusterReset() {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(RESET_WAIT_MS);
        java.util.Set<String> pending = new java.util.HashSet<>();
        for (var r : clusterConfig.replicas) pending.add(r.id);
        while (System.nanoTime() < deadline && !pending.isEmpty()) {
            java.util.Iterator<String> it = pending.iterator();
            while (it.hasNext()) {
                String id = it.next();
                var stub = adminStubs.get(id);
                if (stub == null) { it.remove(); continue; }
                try {
                    PrintLogReply reply = adminStubWithDeadline(stub).printLog(Empty.getDefaultInstance());
                    if (reply.getEntriesCount() == 0) { it.remove(); }
                    else { adminStubWithDeadline(stub).reset(Empty.getDefaultInstance()); }
                } catch (Exception ignored) {}
            }
            if (!pending.isEmpty()) {
                try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            }
        }
        if ("quorum".equalsIgnoreCase(RESET_MODE)) {
            int ok = clusterConfig.replicas.size() - pending.size();
            int need = 2 * f + 1;
            if (ok < need) {
                System.out.println("Warning: only " + ok + " replicas confirmed reset (need >= " + need + ").");
            }
        } else {
            if (!pending.isEmpty()) {
                System.out.println("Warning: reset not confirmed on: " + pending);
            }
        }
    }

    private String normalizeNodeId(String s) {
        if (s == null || s.isEmpty()) return s;
        if (adminStubs.containsKey(s)) return s;
        boolean digits = true;
        for (int i = 0; i < s.length(); i++) { char c = s.charAt(i); if (c < '0' || c > '9') { digits = false; break; } }
        if (digits) {
            String cand = "n" + s;
            if (adminStubs.containsKey(cand)) return cand;
        }
        return s;
    }

    private boolean handleCli(String line) {
        try {
            String[] tok = line.split("\\s+");
            if (tok.length == 0) return false;
            String cmd = tok[0].toLowerCase();
            switch (cmd) {
                case "help" -> {
                    System.out.println("Commands:");
                    System.out.println("  run                - Start current test set or continue to next set");
                    System.out.println("  db [nodeId]        - Print DB of all nodes or a specific node");
                    System.out.println("  log [nodeId]       - Print log of all nodes or a specific node");
                    System.out.println("  completelog [raw|phases] [nodeId] - Default: events; 'raw' for raw event stream; 'phases' for PP/P/C/E");
                    System.out.println("  skip               - Skip current test set or waiting prompt");
                    System.out.println("  goto <set>         - Jump to a specific test set number");
                    System.out.println("  checkpoint [nodeId]- Print checkpoint details of all nodes or a specific node");
                    System.out.println("  status <seq> [view]- Print status for seq across nodes (auto view unless specified)");
                    System.out.println("  view [nodeId]      - Print view messages of all nodes or a specific node");
                    System.out.println("  quit               - Exit");
                    return false;
                }
                case "skip" -> { this.cliSkip.set(true); return false; }
                case "goto" -> {
                    if (tok.length >= 2) {
                        try { this.cliGoto = Integer.parseInt(tok[1]); }
                        catch (NumberFormatException e) { System.out.println("Invalid set number: " + tok[1]); }
                    } else {
                        System.out.println("Usage: goto <set>");
                    }
                    return false;
                }
                case "db" -> {
                    if (tok.length >= 2) printDbOne(normalizeNodeId(tok[1])); else printDbAll();
                    return false;
                }
                case "log" -> {
                    if (tok.length >= 2) printLogOne(normalizeNodeId(tok[1])); else printLogAll();
                    return false;
                }
                case "completelog" -> {
                    String mode = "events"; String target = null;
                    if (tok.length >= 2) {
                        if ("raw".equalsIgnoreCase(tok[1]) || "phases".equalsIgnoreCase(tok[1])) { mode = tok[1].toLowerCase(); if (tok.length >= 3) target = tok[2]; }
                        else { target = tok[1]; }
                    }
                    if (target != null) target = normalizeNodeId(target);
                    if (target != null) printCompleteLogOne(target, mode); else printCompleteLogAll(mode);
                    return false;
                }
                case "checkpoint" -> {
                    if (tok.length >= 2) printCheckpointOne(normalizeNodeId(tok[1])); else printCheckpointAll();
                    return false;
                }
                case "view" -> {
                    if (tok.length >= 2) printViewOne(normalizeNodeId(tok[1])); else printViewAll();
                    return false;
                }
                case "status" -> {
                    if (tok.length == 2) {
                        long seq = Long.parseLong(tok[1]);
                        printStatusAll(0, seq);
                    } else if (tok.length >= 3) {
                        long v = Long.parseLong(tok[1]);
                        long s = Long.parseLong(tok[2]);
                        printStatusAll(v, s);
                    } else {
                        System.out.println("usage: status <seq> [view]");
                    }
                    return false;
                }
                case "quit", "exit" -> { return true; }
                default -> { System.out.println("Unknown command. Type 'help'."); return false; }
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            return false;
        }
    }

    private void printDbAll() {
        StringBuilder header = new StringBuilder();
        header.append(String.format("%-4s", ""));
        for (char c = 'A'; c <= 'J'; c++) header.append(String.format(" %6s", String.valueOf(c)));
        System.out.println(header);
        for (var r : clusterConfig.replicas) {
            var stub = adminStubs.get(r.id);
            if (stub == null) continue;
            long[] vals = new long[10];
            boolean ok = true;
            try {
                PrintDBReply reply = adminStubWithDeadline(stub).printDB(Empty.getDefaultInstance());
                for (var be : reply.getBalancesList()) {
                    if (be.getAccount() != null && be.getAccount().length() == 1) {
                        char acc = be.getAccount().charAt(0);
                        if (acc >= 'A' && acc <= 'J') vals[acc - 'A'] = be.getBalance();
                    }
                }
            } catch (Exception ex) {
                ok = false;
            }
            StringBuilder row = new StringBuilder();
            row.append(String.format("%-4s", r.id));
            if (!ok) {
                for (int i = 0; i < 10; i++) row.append(String.format(" %6s", "ERR"));
            } else {
                for (int i = 0; i < 10; i++) row.append(String.format(" %6d", vals[i]));
            }
            System.out.println(row);
        }
    }

    private void printDbOne(String id) {
        var stub = adminStubs.get(id);
        if (stub == null) { System.out.println("Unknown node: " + id); return; }
        try {
            PrintDBReply reply = adminStubWithDeadline(stub).printDB(Empty.getDefaultInstance());
            System.out.println(id + " DB:");
            printBalanceTable(reply.getBalancesList(), "  ");
        } catch (Exception ex) {
            System.out.println(id + " DB: error: " + ex.getMessage());
        }
    }

    private void printLogAll() {
        for (var r : clusterConfig.replicas) {
            var stub = adminStubs.get(r.id);
            if (stub == null) continue;
            try {
                PrintLogReply reply = adminStubWithDeadline(stub).printLog(Empty.getDefaultInstance());
                System.out.println(r.id + " LOG:");
                for (var e : reply.getEntriesList()) {
                    String phases = String.format("PP=%s P=%s C=%s E=%s",
                            e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                    System.out.println("  (v=" + e.getView() + ", s=" + e.getSeq() + ") client=" + e.getClientId()
                            + "#" + e.getReqSeq() + " status=" + e.getStatus()
                            + " digest=" + e.getRequestDigest()
                            + " op=" + e.getOperation()
                            + " phases[" + phases + "]");
                }
            } catch (Exception ex) {
                System.out.println(r.id + " LOG: error: " + ex.getMessage());
            }
        }
    }

    private void printLogOne(String id) {
        var stub = adminStubs.get(id);
        if (stub == null) { System.out.println("Unknown node: " + id); return; }
        try {
            PrintLogReply reply = adminStubWithDeadline(stub).printLog(Empty.getDefaultInstance());
            System.out.println(id + " LOG:");
            for (var e : reply.getEntriesList()) {
                String phases = String.format("PP=%s P=%s C=%s E=%s",
                        e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                System.out.println("  (v=" + e.getView() + ", s=" + e.getSeq() + ") client=" + e.getClientId()
                        + "#" + e.getReqSeq() + " status=" + e.getStatus()
                        + " digest=" + e.getRequestDigest()
                        + " op=" + e.getOperation()
                        + " phases[" + phases + "]");
            }
        } catch (Exception ex) {
            System.out.println(id + " LOG: error: " + ex.getMessage());
        }
    }

    private void printCompleteLogAll(String mode) {
        for (var r : clusterConfig.replicas) {
            var stub = adminStubs.get(r.id);
            if (stub == null) continue;
            try {
                var reply = adminStubWithDeadline(stub).printCompleteLog(Empty.getDefaultInstance());
                switch ((mode == null ? "events" : mode)) {
                    case "raw" -> {
                        System.out.println("Node " + r.id + " COMPLETE LOG (raw events):");
                        printCompleteLogTable(reply.getEntriesList(), "  ");
                    }
                    case "phases" -> {
                        System.out.println("Node " + r.id + " PHASES:");
                        printStateEntriesTable(reply.getStateEntriesList(), "  ");
                    }
                    default -> {
                        System.out.println("Node " + r.id + " COMPLETE LOG:");
                        printCompleteLogTableDedup(reply.getEntriesList(), "  ");
                    }
                }
            } catch (Exception ex) {
                System.out.println(r.id + " COMPLETE LOG: error: " + ex.getMessage());
            }
        }
    }

    private void printCompleteLogOne(String id, String mode) {
        var stub = adminStubs.get(id);
        if (stub == null) { System.out.println("Unknown node: " + id); return; }
        try {
            var reply = adminStubWithDeadline(stub).printCompleteLog(Empty.getDefaultInstance());
            switch ((mode == null ? "events" : mode)) {
                case "raw" -> {
                    System.out.println("Node " + id + " COMPLETE LOG (raw events):");
                    printCompleteLogTable(reply.getEntriesList(), "  ");
                }
                case "phases" -> {
                    System.out.println("Node " + id + " PHASES:");
                    printStateEntriesTable(reply.getStateEntriesList(), "  ");
                }
                default -> {
                    System.out.println("Node " + id + " COMPLETE LOG:");
                    printCompleteLogTableDedup(reply.getEntriesList(), "  ");
                }
            }
        } catch (Exception ex) {
            System.out.println(id + " COMPLETE LOG: error: " + ex.getMessage());
        }
    }

    private static void printCompleteLogTable(java.util.List<pbft.proto.CompleteLogEntry> entries, String indent) {
        if (entries == null || entries.isEmpty()) { System.out.println(indent + "(empty)"); return; }
        java.util.function.Function<Long,String> fmtTs = ts -> {
            try {
                var inst = java.time.Instant.ofEpochMilli(ts);
                var zdt = java.time.ZonedDateTime.ofInstant(inst, java.time.ZoneId.systemDefault());
                var fmt = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                return zdt.format(fmt);
            } catch (Exception e) { return Long.toString(ts); }
        };
        int wTs = Math.max("Time".length(), entries.stream().map(e -> fmtTs.apply(e.getTsMillis()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wDir = Math.max("Dir".length(), entries.stream().map(e -> e.getDirection().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wType = Math.max("Type".length(), entries.stream().map(e -> e.getType().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wV = Math.max("V".length(), entries.stream().map(e -> Long.toString(e.getView()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wS = Math.max("S".length(), entries.stream().map(e -> Long.toString(e.getSeq()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wFrom = Math.max("From".length(), entries.stream().map(e -> e.getFrom().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wTo = Math.max("To".length(), entries.stream().map(e -> e.getTo().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wClient = Math.max("Client".length(), entries.stream().map(e -> e.getClientId().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wT = Math.max("t".length(), entries.stream().map(e -> Long.toString(e.getReqSeq()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        java.util.function.Function<String,String> shortDg = dg -> {
            if (dg == null) return ""; String d = dg; return d.length() > 8 ? d.substring(0, 8) : d;
        };
        int wDg = Math.max("Digest".length(), entries.stream().map(e -> shortDg.apply(e.getRequestDigest())).max(java.util.Comparator.comparingInt(String::length)).orElse("").length());
        int wStatus = Math.max("Status".length(), entries.stream().map(e -> e.getStatus().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wNotes = Math.max("Notes".length(), entries.stream().map(e -> e.getNotes().length()).max(java.util.Comparator.naturalOrder()).orElse(0));

        String sep = indent + "+" + "-".repeat(wTs+2) + "+" + "-".repeat(wDir+2) + "+" + "-".repeat(wType+2) + "+" + "-".repeat(wV+2) + "+" + "-".repeat(wS+2) + "+" + "-".repeat(wFrom+2) + "+" + "-".repeat(wTo+2) + "+" + "-".repeat(wClient+2) + "+" + "-".repeat(wT+2) + "+" + "-".repeat(wDg+2) + "+" + "-".repeat(wStatus+2) + "+" + "-".repeat(wNotes+2) + "+";
        String hdr = String.format(indent + "| %1$-" + wTs + "s | %2$-" + wDir + "s | %3$-" + wType + "s | %4$-" + wV + "s | %5$-" + wS + "s | %6$-" + wFrom + "s | %7$-" + wTo + "s | %8$-" + wClient + "s | %9$-" + wT + "s | %10$-" + wDg + "s | %11$-" + wStatus + "s | %12$-" + wNotes + "s |",
                "Time","Dir","Type","V","S","From","To","Client","t","Digest","Status","Notes");
        System.out.println(sep);
        System.out.println(hdr);
        System.out.println(sep);
        for (var e : entries) {
            String row = String.format(indent + "| %1$-" + wTs + "s | %2$-" + wDir + "s | %3$-" + wType + "s | %4$-" + wV + "s | %5$-" + wS + "s | %6$-" + wFrom + "s | %7$-" + wTo + "s | %8$-" + wClient + "s | %9$-" + wT + "s | %10$-" + wDg + "s | %11$-" + wStatus + "s | %12$-" + wNotes + "s |",
                    fmtTs.apply(e.getTsMillis()), e.getDirection(), e.getType(), Long.toString(e.getView()), Long.toString(e.getSeq()), e.getFrom(), e.getTo(), e.getClientId(), Long.toString(e.getReqSeq()), shortDg.apply(e.getRequestDigest()), e.getStatus(), e.getNotes());
            System.out.println(row);
        }
        System.out.println(sep);
    }

    private static void printCompleteLogTableDedup(java.util.List<pbft.proto.CompleteLogEntry> entries, String indent) {
        if (entries == null || entries.isEmpty()) { System.out.println(indent + "(empty)"); return; }
        java.util.LinkedHashMap<String, pbft.proto.CompleteLogEntry> unique = new java.util.LinkedHashMap<>();
        for (var e : entries) {
            String dg = e.getRequestDigest();
            String key = String.join("|",
                    e.getDirection(), e.getType(), Long.toString(e.getView()), Long.toString(e.getSeq()),
                    e.getFrom(), e.getTo(), e.getClientId(), Long.toString(e.getReqSeq()), dg == null ? "" : dg,
                    e.getStatus(), e.getNotes());
            unique.putIfAbsent(key, e);
        }
        printCompleteLogTable(new java.util.ArrayList<>(unique.values()), indent);
    }

    private static void printStateEntriesTable(java.util.List<pbft.proto.LogEntry> entries, String indent) {
        if (entries == null || entries.isEmpty()) { System.out.println(indent + "(empty)"); return; }
        java.util.function.Function<String,String> shortDg = dg -> {
            if (dg == null) return ""; String d = dg; return d.length() > 8 ? d.substring(0, 8) : d;
        };
        int wV = Math.max("V".length(), entries.stream().map(e -> Long.toString(e.getView()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wS = Math.max("S".length(), entries.stream().map(e -> Long.toString(e.getSeq()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wClient = Math.max("Client".length(), entries.stream().map(e -> e.getClientId().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wT = Math.max("t".length(), entries.stream().map(e -> Long.toString(e.getReqSeq()).length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        int wDg = Math.max("Digest".length(), entries.stream().map(e -> shortDg.apply(e.getRequestDigest())).max(java.util.Comparator.comparingInt(String::length)).orElse("").length());
        int wStatus = Math.max("Status".length(), entries.stream().map(e -> e.getStatus().length()).max(java.util.Comparator.naturalOrder()).orElse(0));
        String sep = indent + "+" + "-".repeat(wV+2) + "+" + "-".repeat(wS+2) + "+" + "-".repeat(wClient+2) + "+" + "-".repeat(wT+2) + "+" + "-".repeat(wDg+2) + "+" + "-".repeat(wStatus+2) + "+-----+-----+-----+-----+";
        String hdr = String.format(indent + "| %1$-" + wV + "s | %2$-" + wS + "s | %3$-" + wClient + "s | %4$-" + wT + "s | %5$-" + wDg + "s | %6$-" + wStatus + "s |  PP |  P  |  C  |  E  |",
                "V","S","Client","t","Digest","Status");
        System.out.println(sep);
        System.out.println(hdr);
        System.out.println(sep);
        for (var e : entries) {
            String row = String.format(indent + "| %1$-" + wV + "s | %2$-" + wS + "s | %3$-" + wClient + "s | %4$-" + wT + "s | %5$-" + wDg + "s | %6$-" + wStatus + "s |  %7$s  |  %8$s  |  %9$s  |  %10$s  |",
                    Long.toString(e.getView()), Long.toString(e.getSeq()), e.getClientId(), Long.toString(e.getReqSeq()), shortDg.apply(e.getRequestDigest()), e.getStatus(),
                    e.getPrePrepared() ? "Y" : "-", e.getPrepared() ? "Y" : "-", e.getCommitted() ? "Y" : "-", e.getExecuted() ? "Y" : "-");
            System.out.println(row);
        }
        System.out.println(sep);
    }

    private void printCheckpointAll() {
        for (var r : clusterConfig.replicas) {
            var stub = adminStubs.get(r.id);
            if (stub == null) continue;
            try {
                PrintCheckpointReply reply = adminStubWithDeadline(stub).printCheckpoint(Empty.getDefaultInstance());
                String dg = Hex.toHexOrEmpty(reply.getLastStableDigest().toByteArray());
                if (dg.length() > 8) dg = dg.substring(0, 8);
                System.out.println(r.id + " CHECKPOINT:");
                System.out.println("  enabled=" + reply.getEnabled());
                System.out.println("  period=" + reply.getPeriod());
                System.out.println("  last_stable_seq=" + reply.getLastStableSeq());
                System.out.println("  last_stable_digest=" + dg);
                System.out.println("  stable_signers=" + reply.getStableSigners());
                if (reply.getStableSignerIdsCount() > 0) {
                    System.out.println("  stable_signer_ids=" + reply.getStableSignerIdsList());
                }
                System.out.println("  has_local_snapshot=" + reply.getHasLocalSnapshot());
                if (reply.getCheckpointEntriesCount() > 0) {
                    System.out.println("  entries (within checkpoint window):");
                    for (var e : reply.getCheckpointEntriesList()) {
                        String phases = String.format("PP=%s P=%s C=%s E=%s",
                                e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                        System.out.println("    (v=" + e.getView() + ", s=" + e.getSeq() + ") client=" + e.getClientId()
                                + "#" + e.getReqSeq() + " status=" + e.getStatus()
                                + " digest=" + e.getRequestDigest()
                                + " op=" + e.getOperation()
                                + " phases[" + phases + "]");
                    }
                }
            } catch (Exception ex) {
                System.out.println(r.id + " CHECKPOINT: error: " + ex.getMessage());
            }
        }
    }

    private void printCheckpointOne(String id) {
        var stub = adminStubs.get(id);
        if (stub == null) { System.out.println("Unknown node: " + id); return; }
        try {
            PrintCheckpointReply reply = adminStubWithDeadline(stub).printCheckpoint(Empty.getDefaultInstance());
            String dg = Hex.toHexOrEmpty(reply.getLastStableDigest().toByteArray());
            if (dg.length() > 8) dg = dg.substring(0, 8);
            System.out.println(id + " CHECKPOINT:");
            System.out.println("  enabled=" + reply.getEnabled());
            System.out.println("  period=" + reply.getPeriod());
            System.out.println("  last_stable_seq=" + reply.getLastStableSeq());
            System.out.println("  last_stable_digest=" + dg);
            System.out.println("  stable_signers=" + reply.getStableSigners());
            if (reply.getStableSignerIdsCount() > 0) {
                System.out.println("  stable_signer_ids=" + reply.getStableSignerIdsList());
            }
            System.out.println("  has_local_snapshot=" + reply.getHasLocalSnapshot());
            if (reply.getCheckpointEntriesCount() > 0) {
                System.out.println("  entries (within checkpoint window):");
                for (var e : reply.getCheckpointEntriesList()) {
                    String phases = String.format("PP=%s P=%s C=%s E=%s",
                            e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                    System.out.println("    (v=" + e.getView() + ", s=" + e.getSeq() + ") client=" + e.getClientId()
                            + "#" + e.getReqSeq() + " status=" + e.getStatus()
                            + " digest=" + e.getRequestDigest()
                            + " op=" + e.getOperation()
                            + " phases[" + phases + "]");
                }
            }
        } catch (Exception ex) {
            System.out.println(id + " CHECKPOINT: error: " + ex.getMessage());
        }
    }

    private void printViewAll() {
        java.util.Set<Long> seen = new java.util.HashSet<>();
        for (var r : clusterConfig.replicas) {
            var stub = adminStubs.get(r.id);
            if (stub == null) continue;
            try {
                ViewLog reply = adminStubWithDeadline(stub).printView(Empty.getDefaultInstance());
                var nvList = reply.getNewViewsFullList();
                var sCov = reply.getSCovList();
                var hCov = reply.getHCovList();
                if (!nvList.isEmpty()) {
                    for (int i = 0; i < nvList.size(); i++) {
                        long vprime = nvList.get(i).getNewView();
                        if (seen.add(vprime)) {
                            Long sVal = (i < sCov.size()) ? sCov.get(i) : null;
                            Long hVal = (i < hCov.size()) ? hCov.get(i) : null;
                            printNewView(nvList.get(i), "  ", sVal == null ? 0L : sVal, hVal == null ? 0L : hVal);
                        }
                    }
                } else if (reply.getHasLast()) {
                    long vprime = reply.getLastNewView().getNewView();
                    if (seen.add(vprime)) {
                        printNewView(reply.getLastNewView(), "  ", reply.getS(), reply.getH());
                    }
                }
            } catch (Exception ex) {
                System.out.println(r.id + " VIEW: error: " + ex.getMessage());
            }
        }
        if (seen.isEmpty()) {
            System.out.println("No view change occurred in this set.");
        }
    }

    private void printViewOne(String id) {
        var stub = adminStubs.get(id);
        if (stub == null) { System.out.println("Unknown node: " + id); return; }
        try {
            ViewLog reply = adminStubWithDeadline(stub).printView(Empty.getDefaultInstance());
            var nvList = reply.getNewViewsFullList();
            var sCov = reply.getSCovList();
            var hCov = reply.getHCovList();
            if (!nvList.isEmpty()) {
                for (int i = 0; i < nvList.size(); i++) {
                    Long sVal = (i < sCov.size()) ? sCov.get(i) : null;
                    Long hVal = (i < hCov.size()) ? hCov.get(i) : null;
                    printNewView(nvList.get(i), "  ", sVal == null ? 0L : sVal, hVal == null ? 0L : hVal);
                }
            } else if (reply.getHasLast()) {
                printNewView(reply.getLastNewView(), "  ", reply.getS(), reply.getH());
            } else {
                System.out.println("No view change occurred in this set on " + id + ".");
            }
        } catch (Exception ex) {
            System.out.println(id + " VIEW: error: " + ex.getMessage());
        }
    }

    private void printNewView(NewView nv, String indent, long s, long h) {
        System.out.println(indent + "NEW-VIEW v'=" + nv.getNewView() + " [s=" + s + ", h=" + h + "]");
        if (nv.getSelectedPrepreparesCount() > 0) {
            System.out.println(indent + "  O-set (selected PrePrepare messages):");
            for (var pp : nv.getSelectedPrepreparesList()) {
                String dg = Hex.toHexOrEmpty(pp.getRequestDigest().toByteArray());
                if (dg.length() > 8) dg = dg.substring(0, 8);
                boolean isNull = dg.equalsIgnoreCase("") || java.util.Arrays.equals(pp.getRequestDigest().toByteArray(), pbft.common.crypto.Digests.sha256("pbft:null".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                String client = (pp.getClientId().isEmpty() ? "-" : (pp.getClientId() + "#" + pp.getReqSeq()));
                String tail = isNull ? "NO-OP" : ("client=" + client);
                System.out.println(indent + "    - sequence=" + pp.getSeq() + ", view=" + pp.getView() + ", digest=" + dg + ", " + tail);
            }
        } else {
            System.out.println(indent + "  O-set: (none)");
        }
        if (nv.getViewChangesCount() > 0) {
            boolean verbose = Boolean.getBoolean("pbft.client.view.verbose");
            System.out.println(indent + "  V-set (2f+1 ViewChange messages):");
            for (var env : nv.getViewChangesList()) {
                var vctr = MessageDecoder.decodeViewChange(env, keyStore);
                if (vctr.ok()) {
                    var vc = vctr.message();
                    if (verbose) {
                        System.out.println(indent + "    - VC signer=" + vctr.signerId() + ", v'=" + vc.getNewView() + ", lastStable=" + vc.getLastStableSeq());
                        for (var pe : vc.getPsetList()) {
                            String dg = Hex.toHexOrEmpty(pe.getRequestDigest().toByteArray());
                            if (dg.length() > 8) dg = dg.substring(0, 8);
                            java.util.List<String> signers = new java.util.ArrayList<>();
                            if (pe.hasProof()) {
                                try {
                                    for (var sp : pe.getProof().getSignedPreparesList()) {
                                        var prTr = MessageDecoder.decodePrepare(sp, keyStore);
                                        if (prTr.ok()) signers.add(prTr.signerId());
                                    }
                                } catch (Exception ignored) {}
                            }
                            String client = (pe.getClientId().isEmpty() ? "-" : (pe.getClientId() + "#" + pe.getReqSeq()));
                            System.out.println(indent + "      prepared-entry sequence=" + pe.getSeq() + ", view=" + pe.getView() + ", digest=" + dg + ", client=" + client + ", proof_signers=" + signers);
                        }
                    } else {
                        System.out.println(indent + "    - VC signer=" + vctr.signerId() + ", v'=" + vc.getNewView() + ", lastStable=" + vc.getLastStableSeq() + ", prepared_count=" + vc.getPsetCount());
                        for (var pe : vc.getPsetList()) {
                            String dg = Hex.toHexOrEmpty(pe.getRequestDigest().toByteArray());
                            if (dg.length() > 8) dg = dg.substring(0, 8);
                            int proofN = pe.hasProof() ? pe.getProof().getSignedPreparesCount() : 0;
                            String client = (pe.getClientId().isEmpty() ? "-" : (pe.getClientId() + "#" + pe.getReqSeq()));
                            System.out.println(indent + "      prepared-entry sequence=" + pe.getSeq() + ", view=" + pe.getView() + ", digest=" + dg + ", client=" + client + ", proof_count=" + proofN);
                        }
                    }
                } else {
                    System.out.println(indent + "    - [invalid VC envelope]");
                }
            }
        }
    }

    private String primaryId(long view) {
        long idx = ((view - 1) % n) + 1;
        return "n" + idx;
    }

    private void printStatusAll(long view, long seq) {
        String header = (view > 0)
                ? "STATUS(v=" + view + ", s=" + seq + ") across replicas:"
                : "STATUS(s=" + seq + ", auto-view) across replicas:";
        System.out.println(header);
        var stubOpt = adminStubs.values().stream().findFirst();
        if (stubOpt.isEmpty()) {
            System.out.println("  no admin stubs available");
            return;
        }
        try {
            StatusReply reply = adminStubWithDeadline(stubOpt.get()).printStatus(
                    SeqQuery.newBuilder().setView(view).setSeq(seq).setAggregate(true).build());
            for (var s : reply.getStatusesList()) {
                System.out.println("  " + s.getReplicaId() + ": " + s.getStatus());
            }
        } catch (Exception ex) {
            System.out.println("  error: " + ex.getMessage());
        }
    }

    private static void printBalanceTable(List<BalanceEntry> balances, String indent) {
        if (balances.isEmpty()) {
            System.out.println(indent + "(empty)");
            return;
        }
        int accountWidth = Math.max("Account".length(),
                balances.stream().map(b -> b.getAccount().length()).max(Comparator.naturalOrder()).orElse(0));
        int balanceWidth = Math.max("Balance".length(),
                balances.stream().map(b -> Long.toString(b.getBalance()).length()).max(Comparator.naturalOrder()).orElse(0));

        String horizontal = indent + "+" + "-".repeat(accountWidth + 2) + "+" + "-".repeat(balanceWidth + 2) + "+";
        String header = String.format(indent + "| %-" + accountWidth + "s | %-" + balanceWidth + "s |", "Account", "Balance");

        System.out.println(horizontal);
        System.out.println(header);
        System.out.println(horizontal);
        for (var b : balances) {
            String row = String.format(indent + "| %-" + accountWidth + "s | %-" + balanceWidth + "s |",
                    b.getAccount(), Long.toString(b.getBalance()));
            System.out.println(row);
        }
        System.out.println(horizontal);
    }

    private static final Pattern TXN_TRANSFER = Pattern.compile("^\\((?<s>[A-J]),\\s*(?<r>[A-J]),\\s*(?<a>\\d+)\\)\\s*$");
    private static final Pattern TXN_BALANCE  = Pattern.compile("^\\((?<s>[A-J])\\)\\s*$");

    private Txn parseTxn(String s) {
        Matcher m = TXN_TRANSFER.matcher(s);
        if (m.matches()) {
            String from = m.group("s");
            String to = m.group("r");
            long amt = Long.parseLong(m.group("a"));
            return Txn.transfer(from, to, amt);
        }
        m = TXN_BALANCE.matcher(s);
        if (m.matches()) {
            String acc = m.group("s");
            return Txn.balance(acc);
        }
        return null;
    }

    private static final class TestSet { final List<String> txs = new ArrayList<>(); List<String> liveNodes; List<String> byzantineNodes; String attackSpec; }

    private Map<Integer, TestSet> parseCsv(Path path) throws IOException {
        Map<Integer, TestSet> bySet = new LinkedHashMap<>();
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String header = br.readLine();
            String line;
            int lastSet = -1;
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) continue;
                String[] cols = splitCsvLine(line, 5);
                if (cols.length < 2) continue;
                String setStr = stripQuotes(cols[0].trim());
                String txnStr = stripQuotes(cols[1].trim());
                String liveStr = cols.length >= 3 ? stripQuotes(cols[2].trim()) : "";
                String byzStr  = cols.length >= 4 ? stripQuotes(cols[3].trim()) : "";
                String atkStr  = cols.length >= 5 ? stripQuotes(cols[4].trim()) : "";
                if (txnStr.isEmpty()) {
                    String maybeTxn = setStr;
                    if (!maybeTxn.isEmpty() && maybeTxn.startsWith("(") && maybeTxn.endsWith(")")) {
                        txnStr = maybeTxn;
                        setStr = "";
                    } else {
                        continue;
                    }
                }
                int set;
                if (setStr.isEmpty()) {
                    if (lastSet < 0) continue;
                    set = lastSet;
                } else {
                    set = Integer.parseInt(setStr);
                    lastSet = set;
                }
                TestSet spec = bySet.computeIfAbsent(set, k -> new TestSet());
                if (!txnStr.isEmpty()) spec.txs.add(txnStr);
                if (!liveStr.isEmpty()) spec.liveNodes = parseLiveNodes(liveStr);
                if (!byzStr.isEmpty()) spec.byzantineNodes = parseNodeList(byzStr);
                if (!atkStr.isEmpty()) spec.attackSpec = atkStr;
            }
        }
        return bySet;
    }

    private List<String> parseLiveNodes(String s) {
        String raw = s.trim();
        if (raw.startsWith("[") && raw.endsWith("]")) raw = raw.substring(1, raw.length()-1);
        String[] parts = raw.split(",");
        List<String> ids = new ArrayList<>();
        for (String p : parts) {
            String id = p.trim();
            if (!id.isEmpty()) ids.add(id);
        }
        if (ids.isEmpty()) {
            for (var r : clusterConfig.replicas) ids.add(r.id);
        }
        return ids;
    }

    private List<String> parseNodeList(String s) {
        String raw = (s == null) ? "" : s.trim();
        if (raw.isEmpty()) return java.util.Collections.emptyList();
        if (raw.startsWith("[") && raw.endsWith("]")) raw = raw.substring(1, raw.length()-1);
        raw = raw.trim();
        if (raw.isEmpty()) return java.util.Collections.emptyList();
        String[] parts = raw.split(",");
        List<String> ids = new ArrayList<>();
        for (String p : parts) {
            String id = p.trim();
            if (!id.isEmpty()) ids.add(id);
        }
        return ids;
    }

    private void applyAttackConfig(TestSet spec) {
        java.util.Set<String> byz = new java.util.HashSet<>();
        if (spec.byzantineNodes != null) byz.addAll(spec.byzantineNodes);
        java.util.List<String> types = new java.util.ArrayList<>();
        java.util.List<String> darkTargets = new java.util.ArrayList<>();
        java.util.List<String> equivTargets = new java.util.ArrayList<>();
        if (spec.attackSpec != null && !spec.attackSpec.isBlank()) {
            String raw = spec.attackSpec.trim();
            if (raw.startsWith("[") && raw.endsWith("]")) {
                raw = raw.substring(1, raw.length()-1);
            }
            if (!raw.isBlank()) for (String t : splitAttacks(raw)) {
                String tok = t.trim();
                if (tok.isEmpty()) continue;
                if (tok.equalsIgnoreCase("sign")) { if (!types.contains("sign")) types.add("sign"); continue; }
                if (tok.equalsIgnoreCase("crash")) { if (!types.contains("crash")) types.add("crash"); continue; }
                if (tok.equalsIgnoreCase("failstop") || tok.equalsIgnoreCase("fail-stop")) {
                    if (!types.contains("failstop")) types.add("failstop");
                    continue;
                }
                if (tok.equalsIgnoreCase("time"))  { if (!types.contains("time"))  types.add("time");  continue; }
                if (tok.toLowerCase().startsWith("dark")) {
                    int l = tok.indexOf('('); int r = tok.lastIndexOf(')');
                    if (l >= 0 && r > l) {
                        String inside = tok.substring(l+1, r);
                        for (String id : inside.split(",")) { String x = id.trim(); if (!x.isEmpty()) darkTargets.add(x); }
                    }
                    if (!types.contains("dark")) types.add("dark");
                    continue;
                }
                if (tok.toLowerCase().startsWith("failstop") || tok.toLowerCase().startsWith("fail-stop")) {
                    int l = tok.indexOf('('); int r = tok.lastIndexOf(')');
                    if (l >= 0 && r > l) {
                        String inside = tok.substring(l+1, r);
                        for (String id : inside.split(",")) {
                            String x = id.trim();
                            if (!x.isEmpty()) byz.add(x);
                        }
                    }
                    if (!types.contains("failstop")) types.add("failstop");
                    continue;
                }
                if (tok.toLowerCase().startsWith("equivocation")) {
                    int l = tok.indexOf('('); int r = tok.lastIndexOf(')');
                    if (l >= 0 && r > l) {
                        String inside = tok.substring(l+1, r);
                        for (String id : inside.split(",")) { String x = id.trim(); if (!x.isEmpty()) equivTargets.add(x); }
                    }
                    if (!types.contains("equivocation")) types.add("equivocation");
                }
            }
        }
        final var atkTypes = java.util.List.copyOf(types);
        final var atkDark = java.util.List.copyOf(darkTargets);
        final var atkEquiv = java.util.List.copyOf(equivTargets);
        adminStubs.entrySet().parallelStream().forEach(e -> {
            try {
                boolean isByz = byz.contains(e.getKey());
                AttackSpec req = AttackSpec.newBuilder()
                        .setByzantine(isByz)
                        .addAllTypes(atkTypes)
                        .addAllDarkTargets(atkDark)
                        .addAllEquivTargets(atkEquiv)
                        .setTimeDelayMs(0)
                        .build();
                adminStubWithDeadline(e.getValue()).setAttack(req);
            } catch (Exception ignored) {}
        });
    }

    private void applyLiveNodes(List<String> ids) {
        List<String> live = effectiveLiveNodes(ids);
        List<String> reachable = new ArrayList<>();
        List<String> unreachable = new ArrayList<>();
        SeqQuery pingReq = SeqQuery.newBuilder().setView(leaderView.get()).setSeq(0).setAggregate(false).build();
        for (String id : live) {
            var stub = adminStubs.get(id);
            if (stub == null) continue;
            try {
                adminStubWithDeadline(stub).printStatus(pingReq);
                reachable.add(id);
            } catch (Exception ex) {
                unreachable.add(id);
            }
        }
        boolean prune = Boolean.parseBoolean(System.getProperty("pbft.client.live_prune", "false"));
        if (!unreachable.isEmpty()) {
            if (prune) {
                System.out.println("Warning: dropping unreachable replicas from live set: " + unreachable);
            } else {
                System.out.println("Warning: unreachable replicas will be kept in live set: " + unreachable);
            }
        }
        if (reachable.isEmpty()) {
            System.out.println("Error: no reachable replicas from declared live list; PBFT will not make progress.");
        }
        int required = 2 * f + 1;
        if (reachable.size() < required) {
            System.out.println("Warning: only " + reachable.size() + " reachable replicas (need >= " + required + " for quorum).");
        }
        if (!reachable.contains(leaderId) && !reachable.isEmpty()) {
            leaderId = reachable.get(0);
        }
        var idsToSend = prune ? reachable : live;
        quorumPossible = idsToSend.size() >= required;
        var req = LiveNodesRequest.newBuilder().addAllNodeIds(idsToSend).build();
        adminStubs.entrySet().parallelStream().forEach(e -> {
            try { adminStubWithDeadline(e.getValue()).setLiveNodes(req); } catch (Exception ignored) {}
        });
    }

    private List<String> effectiveLiveNodes(List<String> ids) {
        if (ids != null && !ids.isEmpty()) return ids;
        List<String> all = new ArrayList<>();
        for (var r : clusterConfig.replicas) all.add(r.id);
        return all;
    }

    private void awaitClusterReady(List<String> liveIds) {
        List<String> targets = effectiveLiveNodes(liveIds);
        long maxWaitMs;
        try { maxWaitMs = Long.getLong("pbft.client.ready_deadline_ms", 4000L); } catch (Exception ignore) { maxWaitMs = 4000L; }
        long pollMs;
        try { pollMs = Long.getLong("pbft.client.ready_poll_ms", 100L); } catch (Exception ignore) { pollMs = 100L; }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.max(500L, maxWaitMs));
        for (String id : targets) {
            var stub = adminStubs.get(id);
            if (stub == null) continue;
            boolean ok = false;
            while (System.nanoTime() < deadline) {
                try {
                    adminStubWithDeadline(stub).printStatus(
                            SeqQuery.newBuilder().setView(0).setSeq(0).setAggregate(false).build());
                    ok = true;
                    break;
                } catch (Exception ignored) {
                    try { Thread.sleep(pollMs); } catch (InterruptedException ignored2) {}
                }
            }
            if (!ok) {
                System.out.println("Warning: replica " + id + " did not acknowledge readiness within " + maxWaitMs + " ms.");
            }
        }
    }

    private void warmupLeader(List<String> liveIds) {
        boolean enabled = Boolean.parseBoolean(System.getProperty("pbft.client.warmup.enabled", "true"));
        if (!enabled) return;
        String leader = leaderId;
        if (leader == null || leader.isBlank()) leader = primaryId(leaderView.get());
        if (leader == null || leader.isBlank()) return;
        if (liveIds != null && !liveIds.isEmpty() && !liveIds.contains(leader)) {
            leader = liveIds.get(0);
        }
        var stub = nodeStubs.get(leader);
        if (stub == null) return;
        try {
            stub.withDeadlineAfter(CLIENT_RPC_DEADLINE_MS, TimeUnit.MILLISECONDS)
                    .healthCheck(HealthCheckRequest.newBuilder().setWho("warmup").build());
        } catch (Exception ignored) {}
    }

    private String stripQuotes(String s) {
        if (s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length()-1);
        }
        return s;
    }

    private java.util.List<String> splitAttacks(String s) {
        java.util.List<String> out = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        int paren = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') paren++;
            else if (c == ')') paren = Math.max(0, paren-1);
            if ((c == ';' || c == ',') && paren == 0) {
                String tok = cur.toString().trim();
                if (!tok.isEmpty()) out.add(tok);
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        String tok = cur.toString().trim();
        if (!tok.isEmpty()) out.add(tok);
        return out;
    }

    private String[] splitCsvLine(String line, int maxCols) {
        List<String> cols = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        int paren = 0;
        int bracket = 0;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '(') paren++;
            else if (c == ')') paren = Math.max(0, paren-1);
            else if (c == '[') bracket++;
            else if (c == ']') bracket = Math.max(0, bracket-1);
            if (c == ',' && paren == 0 && bracket == 0) {
                cols.add(cur.toString());
                cur.setLength(0);
                if (cols.size() >= maxCols-1) {
                    cur.append(line, i+1, line.length());
                    break;
                }
            } else {
                cur.append(c);
            }
        }
        cols.add(cur.toString());
        return cols.toArray(new String[0]);
    }

    @Override
    public void close() {
        if (server != null) {
            server.shutdownNow();
            try { server.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        try { timers.shutdownNow(); } catch (Exception ignored) {}
        for (var ch : nodeChannels.values()) {
            try { ch.shutdownNow().awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
    }

    private static final class Txn {
        enum Type { TRANSFER, BALANCE }
        final Type type;
        final String from;
        final String to;
        final long amount;
        final String clientId;
        private Txn(Type t, String from, String to, long amount) {
            this.type = t; this.from = from; this.to = to; this.amount = amount; this.clientId = from;
        }
        static Txn transfer(String s, String r, long amt) { return new Txn(Type.TRANSFER, s, r, amt); }
        static Txn balance(String s) { return new Txn(Type.BALANCE, s, null, 0); }
        String clientId() { return clientId; }
    }
}
