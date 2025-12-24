package pbft.replica.core;

import pbft.proto.PrePrepare;
import pbft.proto.ClientRequest;
import pbft.proto.SignedMessage;
import pbft.proto.ClientReply;
import pbft.proto.PreparedEntry;
import pbft.proto.PrepareProof;
import pbft.proto.NewView;
import com.google.protobuf.ByteString;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CompletableFuture;
import java.util.Arrays;

public final class ReplicaState {
    public static final class EntryKey {
        public final long view;
        public final long seq;
        public EntryKey(long v, long s) { this.view = v; this.seq = s; }
        @Override public boolean equals(Object o) { if (!(o instanceof EntryKey k)) return false; return k.view==view && k.seq==seq; }
        @Override public int hashCode() { return Long.hashCode(view)*31 + Long.hashCode(seq); }
        @Override public String toString() { return "("+view+","+seq+")"; }
    }

    public static final class Entry {
        public volatile PrePrepare prePrepare;
        public final Set<String> prepares = Collections.newSetFromMap(new ConcurrentHashMap<>());
        public final Set<String> commits  = Collections.newSetFromMap(new ConcurrentHashMap<>());
        public final ConcurrentMap<String, SignedMessage> prepareSignedMsgs = new ConcurrentHashMap<>();
        public final ConcurrentMap<String, SignedMessage> commitSignedMsgs  = new ConcurrentHashMap<>();
        public volatile byte[] requestDigest;
        public volatile PrepareProof prepareProof;
        public volatile boolean requestFetchInProgress = false;


        public volatile ClientRequest clientRequest;
        public volatile SignedMessage clientRequestSignedMsg;
        public final CompletableFuture<Void> committed = new CompletableFuture<>();
        public volatile boolean prepareProofSent = false;
        public volatile boolean commitProofSent = false;
        public volatile ClientReply finalReply;
        public volatile SignedMessage finalReplySignedMsg;

        public volatile boolean prePrepared = false;
        public volatile boolean prepared = false;
        public volatile boolean committedFlag = false;
        public volatile boolean executed = false;
    }

    private final ConcurrentMap<EntryKey, Entry> log = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, EntryKey> byClientReq = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, ConcurrentHashMap<String, SignedMessage>> viewChanges = new ConcurrentHashMap<>();
    private final java.util.Set<Long> sentNewView = java.util.Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final java.util.concurrent.CopyOnWriteArrayList<String> viewLog = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final ConcurrentMap<String, ClientReply> executedReplies = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, SignedMessage> executedReplyEnvelopes = new ConcurrentHashMap<>();
    private volatile NewView lastNewView;
    private volatile long lastNewViewS = 0L;
    private volatile long lastNewViewH = 0L;
    private final java.util.concurrent.CopyOnWriteArrayList<NewView> newViewHistory = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.CopyOnWriteArrayList<Long> sCovHistory = new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.CopyOnWriteArrayList<Long> hCovHistory = new java.util.concurrent.CopyOnWriteArrayList<>();

    private final ConcurrentMap<Long, ConcurrentHashMap<String, ConcurrentHashMap<String, SignedMessage>>> checkpointMsgs = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, byte[]> localCheckpointSnapshots = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, byte[]> checkpointFetchInProgress = new ConcurrentHashMap<>();
    private volatile byte[] stableCheckpointDigest = null;
    private volatile java.util.List<SignedMessage> stableCheckpointCert = java.util.Collections.emptyList();

    private final AtomicLong currentView = new AtomicLong(1);
    private final AtomicLong nextSeq = new AtomicLong(1);
    private final AtomicLong nextExecSeq = new AtomicLong(1);
    public volatile boolean viewChangeActive = false;
    public volatile long pendingNewView = 0;
    public volatile long lastStableSeq = 0;

    public long currentView() { return currentView.get(); }
    public void setCurrentView(long v) { currentView.set(v); }

    public long allocSeq() { return nextSeq.getAndIncrement(); }
    public void setNextSeq(long s) { nextSeq.set(s); }
    public long nextExecSeq() { return nextExecSeq.get(); }
    public void advanceExecSeq() { nextExecSeq.incrementAndGet(); }

    public Entry getOrCreate(long view, long seq) { return log.computeIfAbsent(new EntryKey(view, seq), k -> new Entry()); }
    public Entry get(long view, long seq) { return log.get(new EntryKey(view, seq)); }
    public java.util.Map<EntryKey, Entry> snapshotLog() { return new java.util.HashMap<>(log); }
    public EntryKey latestEntryKeyForSeq(long seq) {
        EntryKey best = null;
        for (var e : log.entrySet()) {
            EntryKey key = e.getKey();
            if (key.seq != seq) continue;
            if (best == null || key.view > best.view) best = key;
        }
        return best;
    }
    public java.util.Map<String, SignedMessage> viewChangesFor(long vPrime) { return viewChanges.computeIfAbsent(vPrime, k -> new ConcurrentHashMap<>()); }
    public void recordViewChange(long vPrime, String signer, SignedMessage env) { viewChangesFor(vPrime).put(signer, env); }
    public Entry removeEntry(EntryKey key) { return key == null ? null : log.remove(key); }
    public boolean hasSentNewView(long vPrime) { return sentNewView.contains(vPrime); }
    public void markNewViewSent(long vPrime) { sentNewView.add(vPrime); }
    public void appendViewLog(String entry) { if (entry != null && !entry.isEmpty()) viewLog.add(entry); }
    public java.util.List<String> snapshotViewLog() { return new java.util.ArrayList<>(viewLog); }
    public void setLastNewView(NewView nv, long s, long h) { this.lastNewView = nv; this.lastNewViewS = s; this.lastNewViewH = h; }
    public NewView snapshotLastNewView() { return lastNewView; }
    public long lastNewViewS() { return lastNewViewS; }
    public long lastNewViewH() { return lastNewViewH; }
    public void appendNewViewHistory(NewView nv, long s, long h) { if (nv != null) { newViewHistory.add(nv); sCovHistory.add(s); hCovHistory.add(h); } }
    public java.util.List<NewView> snapshotNewViewHistory() { return new java.util.ArrayList<>(newViewHistory); }
    public java.util.List<Long> snapshotSCovHistory() { return new java.util.ArrayList<>(sCovHistory); }
    public java.util.List<Long> snapshotHCovHistory() { return new java.util.ArrayList<>(hCovHistory); }

    public java.util.List<PreparedEntry> exportPreparedEntries(long minSeqExclusive) {
        java.util.ArrayList<PreparedEntry> list = new java.util.ArrayList<>();
        for (var e : log.entrySet()) {
            long v = e.getKey().view;
            long s = e.getKey().seq;
            var en = e.getValue();
            if (!en.prepared) continue;
            if (s <= minSeqExclusive) continue;
            String cid = "";
            long t = 0L;
            if (en.clientRequest != null) { cid = en.clientRequest.getClientId(); t = en.clientRequest.getReqSeq(); }
            else if (en.prePrepare != null) { cid = en.prePrepare.getClientId(); t = en.prePrepare.getReqSeq(); }
            var peb = PreparedEntry.newBuilder()
                    .setView(v)
                    .setSeq(s)
                    .setRequestDigest(com.google.protobuf.ByteString.copyFrom(en.requestDigest == null ? new byte[0] : en.requestDigest))
                    .setClientId(cid)
                    .setReqSeq(t);

            if (en.prepareProof != null) {
                peb.setProof(en.prepareProof);
            } else if (!en.prepareSignedMsgs.isEmpty()) {
                PrepareProof proof = PrepareProof.newBuilder()
                        .setView(v)
                        .setSeq(s)
                        .setRequestDigest(com.google.protobuf.ByteString.copyFrom(en.requestDigest == null ? new byte[0] : en.requestDigest))
                        .addAllSignedPrepares(en.prepareSignedMsgs.values())
                        .build();
                peb.setProof(proof);
            }
            list.add(peb.build());
        }
        list.sort(java.util.Comparator.comparingLong(PreparedEntry::getSeq));
        return list;
    }

    public void indexClientReq(String clientId, long reqSeq, EntryKey key) {
        byClientReq.put(clientKey(clientId, reqSeq), key);
    }
    public EntryKey findByClientReq(String clientId, long reqSeq) {
        return byClientReq.get(clientKey(clientId, reqSeq));
    }
    private String clientKey(String clientId, long reqSeq) { return clientId + "#" + reqSeq; }

    public void unindexClientReq(String clientId, long reqSeq) {
        byClientReq.remove(clientKey(clientId, reqSeq));
    }

    public boolean hasExecuted(String clientId, long reqSeq) {
        return executedReplies.containsKey(clientKey(clientId, reqSeq));
    }
    public ClientReply getExecutedReply(String clientId, long reqSeq) {
        return executedReplies.get(clientKey(clientId, reqSeq));
    }
    public SignedMessage getExecutedReplyEnvelope(String clientId, long reqSeq) {
        return executedReplyEnvelopes.get(clientKey(clientId, reqSeq));
    }
    public void recordExecutedReply(String clientId, long reqSeq, ClientReply reply, SignedMessage envelope) {
        String key = clientKey(clientId, reqSeq);
        if (reply != null) executedReplies.put(key, reply);
        if (envelope != null) executedReplyEnvelopes.put(key, envelope);
    }

    public void resetForNextSet(long initialView) {
        log.clear();
        byClientReq.clear();
        executedReplies.clear();
        executedReplyEnvelopes.clear();
        currentView.set(initialView);
        nextSeq.set(1);
        nextExecSeq.set(1);
        viewChanges.clear();
        sentNewView.clear();
        viewLog.clear();
        lastNewView = null;
        lastNewViewS = 0L;
        lastNewViewH = 0L;
        newViewHistory.clear();
        sCovHistory.clear();
        hCovHistory.clear();
        viewChangeActive = false;
        pendingNewView = 0;
        lastStableSeq = 0;
        stableCheckpointDigest = null;
        stableCheckpointCert = java.util.Collections.emptyList();
        checkpointMsgs.clear();
        localCheckpointSnapshots.clear();
        checkpointFetchInProgress.clear();
        completeLog.clear();
    }

    public void pruneViewChangesUpTo(long vMax) {
        viewChanges.keySet().removeIf(v -> v <= vMax);
        sentNewView.removeIf(v -> v <= vMax);
    }

    public static boolean setOrVerifyDigest(Entry entry, byte[] digest) {
        if (digest == null) return false;
        byte[] current = entry.requestDigest;
        if (current == null) {
            entry.requestDigest = Arrays.copyOf(digest, digest.length);
            return true;
        }
        return Arrays.equals(current, digest);
    }

    private static String k(byte[] digest) { return java.util.Base64.getEncoder().encodeToString(digest == null ? new byte[0] : digest); }

    public void recordLocalCheckpoint(long seq, byte[] digest, SignedMessage env, byte[] snapshotBytes) {
        if (seq <= 0 || digest == null) return;
        localCheckpointSnapshots.put(seq, snapshotBytes);
        recordCheckpoint(seq, digest, env);
    }

    public void recordCheckpoint(long seq, byte[] digest, SignedMessage env) {
        if (seq <= 0 || digest == null || env == null) return;
        var byDigest = checkpointMsgs.computeIfAbsent(seq, _k -> new ConcurrentHashMap<>());
        var bySigner = byDigest.computeIfAbsent(k(digest), _k -> new ConcurrentHashMap<>());
        bySigner.putIfAbsent(env.getSignerId(), env);
    }

    public int uniqueCheckpointSignerCount(long seq, byte[] digest) {
        var byDigest = checkpointMsgs.get(seq);
        if (byDigest == null) return 0;
        var bySigner = byDigest.get(k(digest));
        return bySigner == null ? 0 : bySigner.size();
    }

    public java.util.List<SignedMessage> checkpointEnvelopes(long seq, byte[] digest) {
        var byDigest = checkpointMsgs.get(seq);
        if (byDigest == null) return java.util.Collections.emptyList();
        var bySigner = byDigest.get(k(digest));
        if (bySigner == null) return java.util.Collections.emptyList();
        return new java.util.ArrayList<>(bySigner.values());
    }

    public byte[] localCheckpointSnapshot(long seq) { return localCheckpointSnapshots.get(seq); }

    public void setStableCheckpoint(long seq, byte[] digest, java.util.List<SignedMessage> cert) {
        if (seq <= lastStableSeq) return;
        lastStableSeq = seq;
        stableCheckpointDigest = (digest == null) ? null : java.util.Arrays.copyOf(digest, digest.length);
        stableCheckpointCert = (cert == null) ? java.util.Collections.emptyList() : new java.util.ArrayList<>(cert);
        nextExecSeq.updateAndGet(cur -> Math.max(cur, seq + 1));
        checkpointFetchInProgress.remove(seq);
        for (var e : log.entrySet()) {
            long s = e.getKey().seq;
            if (s <= seq) {
                Entry en = e.getValue();
                en.requestFetchInProgress = false;
                if (!en.committedFlag) {
                    en.committedFlag = true;
                }
                if (!en.prepared) {
                    en.prepared = true;
                }
                if (!en.executed) {
                    en.executed = true;
                }
                if (!en.committed.isDone()) {
                    en.committed.complete(null);
                }
            }
        }
    }

    public ByteString stableCheckpointDigestBytes() {
        return stableCheckpointDigest == null ? ByteString.EMPTY : ByteString.copyFrom(stableCheckpointDigest);
    }

    public java.util.List<SignedMessage> stableCheckpointCertificate() { return new java.util.ArrayList<>(stableCheckpointCert); }

    public boolean matchesStableDigest(long seq, byte[] digest) {
        if (seq != lastStableSeq || digest == null || stableCheckpointDigest == null) return false;
        return java.util.Arrays.equals(stableCheckpointDigest, digest);
    }

    public boolean markCheckpointFetch(long seq, byte[] digest) {
        if (seq <= 0 || digest == null) return false;
        return checkpointFetchInProgress.putIfAbsent(seq, java.util.Arrays.copyOf(digest, digest.length)) == null;
    }

    public void clearCheckpointFetch(long seq, byte[] digest) {
        if (seq <= 0) return;
        byte[] current = checkpointFetchInProgress.get(seq);
        if (current != null && digest != null && java.util.Arrays.equals(current, digest)) {
            checkpointFetchInProgress.remove(seq, current);
        }
    }

    public static final class CompleteEvent {
        public final long tsMillis;
        public final String direction;
        public final String type;
        public final long view;
        public final long seq;
        public final String from;
        public final String to;
        public final String clientId;
        public final long reqSeq;
        public final byte[] requestDigest;
        public final String status;
        public final String notes;

        public CompleteEvent(long tsMillis,
                              String direction,
                              String type,
                              long view,
                              long seq,
                              String from,
                              String to,
                              String clientId,
                              long reqSeq,
                              byte[] requestDigest,
                              String status,
                              String notes) {
            this.tsMillis = tsMillis;
            this.direction = direction;
            this.type = type;
            this.view = view;
            this.seq = seq;
            this.from = from;
            this.to = to;
            this.clientId = clientId == null ? "" : clientId;
            this.reqSeq = reqSeq;
            this.requestDigest = requestDigest == null ? null : java.util.Arrays.copyOf(requestDigest, requestDigest.length);
            this.status = status == null ? "" : status;
            this.notes = notes == null ? "" : notes;
        }
    }

    private final java.util.concurrent.CopyOnWriteArrayList<CompleteEvent> completeLog = new java.util.concurrent.CopyOnWriteArrayList<>();

    public void recordEvent(String direction,
                            String type,
                            long view,
                            long seq,
                            String from,
                            String to,
                            String clientId,
                            long reqSeq,
                            byte[] requestDigest,
                            String status,
                            String notes) {
        completeLog.add(new CompleteEvent(System.currentTimeMillis(), direction, type, view, seq, from, to, clientId, reqSeq, requestDigest, status, notes));
    }

    public java.util.List<CompleteEvent> snapshotCompleteLog() {
        return new java.util.ArrayList<>(completeLog);
    }
}
