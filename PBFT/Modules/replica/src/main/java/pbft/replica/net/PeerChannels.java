package pbft.replica.net;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import pbft.common.config.ClusterConfig;
import pbft.proto.AdminServiceGrpc;
import pbft.proto.ClientServiceGrpc;
import pbft.proto.ReplicaServiceGrpc;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class PeerChannels {
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, ReplicaServiceGrpc.ReplicaServiceBlockingStub> replicaStubs = new ConcurrentHashMap<>();
    private final Map<String, ClientServiceGrpc.ClientServiceBlockingStub> clientStubs = new ConcurrentHashMap<>();
    private final Map<String, AdminServiceGrpc.AdminServiceBlockingStub> adminStubs = new ConcurrentHashMap<>();

    private final List<ClusterConfig.Replica> replicas;
    private final String selfId;
    private final Set<String> livePeers = ConcurrentHashMap.newKeySet();

    public PeerChannels(ClusterConfig cfg, String selfId) {
        this.replicas = cfg.replicas == null ? List.of() : List.copyOf(cfg.replicas);
        this.selfId = selfId;
        for (var r : this.replicas) if (!selfId.equals(r.id)) livePeers.add(r.id);
    }

    private ManagedChannel channelFor(String id) {
        return channels.computeIfAbsent(id, rid -> {
            var r = replicas.stream().filter(x -> x.id.equals(rid)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unknown replica id: " + rid));
            return NettyChannelBuilder.forAddress(r.host, r.port)
                    .usePlaintext()
                    .build();
        });
    }

    public List<String> peerIds() {
        ArrayList<String> ids = new ArrayList<>();
        for (var r : replicas) if (!selfId.equals(r.id)) ids.add(r.id);
        return ids;
    }

    public Map<String, ReplicaServiceGrpc.ReplicaServiceBlockingStub> replicaStubs() {
        Map<String, ReplicaServiceGrpc.ReplicaServiceBlockingStub> m = new HashMap<>();
        for (String id : peerIds()) {
            if (!livePeers.contains(id)) continue;
            m.put(id, replicaStubs.computeIfAbsent(id, rid -> ReplicaServiceGrpc.newBlockingStub(channelFor(rid))));
        }
        return Collections.unmodifiableMap(m);
    }

    public Map<String, ClientServiceGrpc.ClientServiceBlockingStub> clientStubs() {
        Map<String, ClientServiceGrpc.ClientServiceBlockingStub> m = new HashMap<>();
        for (String id : peerIds()) {
            if (!livePeers.contains(id)) continue;
            m.put(id, clientStubs.computeIfAbsent(id, rid -> ClientServiceGrpc.newBlockingStub(channelFor(rid))));
        }
        return Collections.unmodifiableMap(m);
    }

    public Map<String, AdminServiceGrpc.AdminServiceBlockingStub> adminStubs() {
        for (String id : peerIds()) {
            adminStubs.computeIfAbsent(id, rid -> AdminServiceGrpc.newBlockingStub(channelFor(rid)));
        }
        return Collections.unmodifiableMap(adminStubs);
    }

    public void shutdown() {
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

    public ReplicaServiceGrpc.ReplicaServiceBlockingStub stubFor(String replicaId) {
        if (selfId.equals(replicaId)) throw new IllegalArgumentException("stubFor self not allowed: " + replicaId);
        if (!livePeers.contains(replicaId)) throw new IllegalStateException("peer not live: " + replicaId);
        return replicaStubs.computeIfAbsent(replicaId, rid -> ReplicaServiceGrpc.newBlockingStub(channelFor(rid)));
    }

    public ClientServiceGrpc.ClientServiceBlockingStub clientStubFor(String replicaId) {
        if (selfId.equals(replicaId)) throw new IllegalArgumentException("clientStubFor self not allowed: " + replicaId);
        if (!livePeers.contains(replicaId)) throw new IllegalStateException("peer not live: " + replicaId);
        return clientStubs.computeIfAbsent(replicaId, rid -> ClientServiceGrpc.newBlockingStub(channelFor(rid)));
    }

    public AdminServiceGrpc.AdminServiceBlockingStub adminStubFor(String replicaId) {
        if (selfId.equals(replicaId)) throw new IllegalArgumentException("adminStubFor self not allowed: " + replicaId);
        return adminStubs.computeIfAbsent(replicaId, rid -> AdminServiceGrpc.newBlockingStub(channelFor(rid)));
    }

    public void setLivePeers(Collection<String> ids) {
        livePeers.clear();
        for (String id : ids) {
            if (!selfId.equals(id)) livePeers.add(id);
        }
    }

    public boolean isLive(String id) { return livePeers.contains(id); }
}
