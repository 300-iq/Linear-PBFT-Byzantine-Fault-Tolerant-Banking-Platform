package pbft.replica.net;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import pbft.common.config.ClusterConfig;
import pbft.proto.ClientReplyServiceGrpc;
import pbft.proto.SignedMessage;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.ConcurrentHashMap;

public final class ClientReplySender implements AutoCloseable {
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, ClientReplyServiceGrpc.ClientReplyServiceBlockingStub> stubs = new ConcurrentHashMap<>();
    private final ExecutorService sendExec = Executors.newFixedThreadPool(2);
    private final ClusterConfig cfg;

    public ClientReplySender(ClusterConfig cfg) {
        this.cfg = cfg;
    }

    private ClientReplyServiceGrpc.ClientReplyServiceBlockingStub stubFor(String clientId) {
        var existing = stubs.get(clientId);
        if (existing != null) return existing;
        if (cfg.clients == null) return null;
        var cOpt = cfg.clients.stream().filter(c -> clientId.equals(c.id)).findFirst();
        var chosen = cOpt.orElseGet(() -> cfg.clients.isEmpty() ? null : cfg.clients.get(0));
        if (chosen == null) return null;
        if (chosen.host == null || chosen.host.isBlank() || chosen.port <= 0) {
            var fallback = cfg.clients.get(0);
            if (fallback == null || fallback.host == null || fallback.host.isBlank() || fallback.port <= 0) return null;
            chosen = fallback;
        }
        final String host = chosen.host;
        final int port = chosen.port;
        ManagedChannel ch = channels.computeIfAbsent(clientId, _k -> NettyChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build());
        var stub = ClientReplyServiceGrpc.newBlockingStub(ch);
        stubs.put(clientId, stub);
        return stub;
    }

    public void deliver(String clientId, SignedMessage reply, long deadlineMs) {
        var stub = stubFor(clientId);
        if (stub == null) return;
        sendExec.submit(() -> {
            try {
                stub.withDeadlineAfter(deadlineMs, TimeUnit.MILLISECONDS).deliver(reply);
            } catch (StatusRuntimeException e) {
            } catch (Exception e) {
            }
        });
    }

    @Override
    public void close() {
        sendExec.shutdownNow();
        for (var ch : channels.values()) {
            ch.shutdown();
        }
        for (var ch : channels.values()) {
            try { ch.awaitTermination(2, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
        }
        for (var ch : channels.values()) {
            if (!ch.isTerminated()) ch.shutdownNow();
        }
    }
}
