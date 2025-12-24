package pbft.replica;

import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pbft.common.crypto.RequestType;
import picocli.CommandLine;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import pbft.common.config.ClusterConfig;
import pbft.common.config.KeyStore;
import pbft.common.crypto.KeyFiles;
import pbft.common.crypto.Signer;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.PublicKey;
import pbft.replica.rpc.RpcSupport.NodeInfo;
import pbft.replica.rpc.ReplicaRpcService;
import pbft.replica.rpc.ClientRpcService;
import pbft.replica.net.PeerChannels;
import pbft.replica.net.ReplicaSender;
import pbft.replica.core.ReplicaState;
import pbft.replica.core.ReplicaContext;
import pbft.replica.core.ConsensusEngine;
import pbft.replica.core.ReplicaTimers;
import pbft.replica.state.BankDB;
import pbft.replica.rpc.AdminRpcService;
import pbft.replica.net.ClientReplySender;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ReplicaMain implements Callable<Integer> {
    static {

        if (System.getProperty("logback.statusListenerClass") == null) {
            System.setProperty("logback.statusListenerClass", "ch.qos.logback.core.status.NopStatusListener");
        }
    }
    private static final Logger log = LoggerFactory.getLogger(ReplicaMain.class);

    @CommandLine.Option(names="--id", required=true) String nodeId;
    @CommandLine.Option(names="--port", required=true) int port;
    @CommandLine.Option(names="--config", required=true, description="Path to configs/cluster.json")
    String configPath;

    @CommandLine.Option(names="--keys-dir", required=true, description="Directory containing ed25519.key for this node")
    String keysDir;

    public static void main(String[] args) {
        System.exit(new CommandLine(new ReplicaMain()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        log.info("Starting Node {} on port {}", nodeId, port);

        java.util.logging.Logger.getLogger("io.grpc").setLevel(java.util.logging.Level.SEVERE);
        java.util.logging.Logger.getLogger("io.grpc.internal").setLevel(java.util.logging.Level.SEVERE);
        java.util.logging.Logger.getLogger("io.grpc.internal.AtomicBackoff").setLevel(java.util.logging.Level.SEVERE);

        Path configFile = Path.of(configPath).toAbsolutePath();
        ClusterConfig cfg = ClusterConfig.load(configFile);
        Path baseDir = configFile.getParent().getParent();

        KeyStore reg = KeyStore.from(cfg, baseDir);

        PrivateKey sk = KeyFiles.loadPrivateKeyPem(Path.of(keysDir, "ed25519.key"));

        if (cfg.n != (3 * cfg.f + 1)) {
            throw new IllegalStateException("Config invalid: n=" + cfg.n + " must equal 3f+1 where f=" + cfg.f);
        }
        if (cfg.initialView < 1) {
            throw new IllegalStateException("Config invalid: initialView must be >= 1");
        }
        if (cfg.replicas == null || cfg.replicas.isEmpty()) {
            throw new IllegalStateException("Config invalid: replicas list empty");
        }
        var meOpt = cfg.replicas.stream().filter(r -> nodeId.equals(r.id)).findFirst();
        if (meOpt.isEmpty()) {
            throw new IllegalStateException("Config invalid: nodeId " + nodeId + " not present in replicas list");
        }
        var me = meOpt.get();
        if (me.port != this.port) {
            throw new IllegalStateException("Port mismatch: CLI --port=" + this.port + " but config has " + me.port + " for " + nodeId);
        }
        if (cfg.n != cfg.replicas.size()) {
            throw new IllegalStateException("Config invalid: n=" + cfg.n + " but replicas.size()=" + cfg.replicas.size());
        }
        if (reg.replicaCount() != cfg.replicas.size()) {
            throw new IllegalStateException("Trust invalid: loaded " + reg.replicaCount() + " replica keys but config has " + cfg.replicas.size());
        }
        if (cfg.clients != null && !cfg.clients.isEmpty() && reg.clientCount() != cfg.clients.size()) {
            throw new IllegalStateException("Trust invalid: loaded " + reg.clientCount() + " client keys but config has " + cfg.clients.size());
        }

        PublicKey myPubFromConfig = KeyFiles.loadPublicKeyPem(baseDir.resolve(me.publicKeyPath));
        byte[] probePayload = new byte[]{1,2,3};
        byte[] sig = Signer.sign(RequestType.CLIENT_REQUEST, "pbft.ClientRequest", probePayload, sk);
        boolean ok = Signer.verify(RequestType.CLIENT_REQUEST, "pbft.ClientRequest", probePayload, sig, myPubFromConfig);
        if (!ok) {
            throw new IllegalStateException("Private key does not match configured public key for " + nodeId);
        }

        log.info("Replica {} trust: replicas={}, clients={}", nodeId, reg.replicaCount(), reg.clientCount());

        NodeInfo nodeInfo = new NodeInfo(nodeId, cfg.n, cfg.f, cfg.initialView, sk, reg);

        PeerChannels peers = new PeerChannels(cfg, nodeId);

        ReplicaState state = new ReplicaState();
        state.setCurrentView(cfg.initialView);
        ReplicaSender sender = new ReplicaSender(nodeInfo, peers);
        boolean dbPersist = Boolean.parseBoolean(System.getProperty("pbft.db.persist", "true"));
        BankDB db;
        if (dbPersist) {
            String csvProp = System.getProperty("pbft.db.csv_path");
            Path csvPath = (csvProp != null && !csvProp.isBlank())
                    ? Path.of(csvProp)
                    : baseDir.resolve("Logs").resolve("db").resolve(nodeId + ".csv");
            long dbDebounce;
            try { String p = System.getProperty("pbft.db.debounce_ms"); dbDebounce = (p != null) ? Long.parseLong(p) : 100L; } catch (Exception ignore) { dbDebounce = 100L; }
            db = new BankDB(csvPath, true, dbDebounce);
        } else {
            db = new BankDB();
        }
        ClientReplySender clientReplies = new ClientReplySender(cfg);
        int outThreads;
        try {
            String p = System.getProperty("pbft.outbound.threads");
            outThreads = (p != null) ? Integer.parseInt(p) : Math.max(4, cfg.n);
        } catch (Exception ignore) { outThreads = Math.max(4, cfg.n); }
        if (outThreads < 2) outThreads = 2;
        ExecutorService outbound = Executors.newFixedThreadPool(outThreads, r -> { Thread t = new Thread(r, nodeId+"-out"); t.setDaemon(true); return t; });
        ReplicaContext context = new ReplicaContext(nodeInfo, state, peers, sender, db, clientReplies, outbound);
        long baseMs;
        try {
            String prop = System.getProperty("pbft.timers.base_ms");
            baseMs = (prop != null) ? Long.parseLong(prop) : 8000L;
        } catch (Exception ignore) { baseMs = 8000L; }
        if (baseMs < 500L) baseMs = 500L;
        ReplicaTimers realTimers = new ReplicaTimers(context, baseMs);
        context.timers = realTimers;
        ConsensusEngine engine = new ConsensusEngine(context);
        context.engine = engine;
        realTimers.onViewStart(cfg.initialView);

        Server server = NettyServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", port))
                .addService(new ReplicaRpcService(nodeInfo, engine, context))
                .addService(new ClientRpcService(nodeInfo, engine, context))
                .addService(new AdminRpcService(engine, context))
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Shutting down replica {}", nodeId);
                server.shutdown();
                peers.shutdown();
                engine.shutdown();
                outbound.shutdownNow();
                try { realTimers.close(); } catch (Exception ignored) {}
                try { clientReplies.close(); } catch (Exception ignored) {}
                try { db.close(); } catch (Exception ignored) {}
            } catch (Exception ignored) {}
        }));
        log.info("Replica {} started", nodeId);
        server.awaitTermination();
        return 0;
    }

}
