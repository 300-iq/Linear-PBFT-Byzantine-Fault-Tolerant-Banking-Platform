package pbft.replica.core;

import pbft.replica.net.PeerChannels;
import pbft.replica.net.ReplicaSender;
import pbft.replica.net.ClientReplySender;
import pbft.replica.rpc.RpcSupport;
import pbft.replica.state.BankDB;
import java.util.concurrent.ExecutorService;
import pbft.replica.core.ConsensusEngine;

public final class ReplicaContext {
    public final RpcSupport.NodeInfo node;
    public final ReplicaState state;
    public final PeerChannels peers;
    public final ReplicaSender sender;
    public final BankDB db;
    public final ClientReplySender clientReplies;
    public final ExecutorService outbound;
    public volatile ReplicaTimers timers;
    public volatile ConsensusEngine engine;
    public volatile boolean liveSelf = true;

    public ReplicaContext(RpcSupport.NodeInfo node,
                          ReplicaState state,
                          PeerChannels peers,
                          ReplicaSender sender,
                          BankDB db,
                          ClientReplySender clientReplies,
                          ExecutorService outbound,
                          ReplicaTimers timers) {
        this.node = node;
        this.state = state;
        this.peers = peers;
        this.sender = sender;
        this.db = db;
        this.clientReplies = clientReplies;
        this.outbound = outbound;
        this.timers = timers;
        this.engine = null;
    }

    public ReplicaContext(RpcSupport.NodeInfo node,
                          ReplicaState state,
                          PeerChannels peers,
                          ReplicaSender sender,
                          BankDB db,
                          ClientReplySender clientReplies,
                          ExecutorService outbound) {
        this(node, state, peers, sender, db, clientReplies, outbound, null);
    }
}
