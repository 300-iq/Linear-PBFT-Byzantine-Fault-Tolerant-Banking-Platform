package pbft.replica.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pbft.replica.rpc.RpcSupport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public final class ConsensusEngine {
    private static final Logger log = LoggerFactory.getLogger(ConsensusEngine.class);
    private final ExecutorService exec;
    private final ReplicaContext ctx;

    public ConsensusEngine(ReplicaContext ctx) {
        this.ctx = ctx;
        this.exec = Executors.newSingleThreadExecutor(new NamedTF(ctx.node.nodeId + "-pbft"));
    }

    public void submit(String reason, Runnable r) {
        exec.submit(() -> {
            try { r.run(); } catch (Throwable t) { log.error("PBFT task failed: {}", reason, t); }
        });
    }

    public void shutdown() { exec.shutdown(); }

    static final class NamedTF implements ThreadFactory {
        private final String base;
        NamedTF(String base) { this.base = base; }
        @Override public Thread newThread(Runnable r) {
            Thread t = new Thread(r, base);
            t.setDaemon(true);
            return t;
        }
    }
}
