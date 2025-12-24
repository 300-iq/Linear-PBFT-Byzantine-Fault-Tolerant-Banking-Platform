package pbft.replica.state;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BankDB {
    private final ConcurrentHashMap<String, Long> checking = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Long> savings = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();
    private final boolean persist;
    private final Path csvPath;
    private final ScheduledExecutorService io;
    private final AtomicBoolean pending = new AtomicBoolean(false);
    private final long debounceMs;

    public BankDB() {
        for (char c = 'A'; c <= 'J'; c++) {
            String k = String.valueOf(c);
            checking.put(k, 10L);
            savings.put(k, 0L);
        }
        this.persist = false;
        this.csvPath = null;
        this.io = null;
        this.debounceMs = 0L;
    }

    public BankDB(Path csvPath, boolean persist, long debounceMs) {
        for (char c = 'A'; c <= 'J'; c++) {
            String k = String.valueOf(c);
            checking.put(k, 10L);
            savings.put(k, 0L);
        }
        this.persist = persist;
        this.csvPath = csvPath;
        this.debounceMs = debounceMs <= 0L ? 50L : debounceMs;
        if (persist) {
            try { if (csvPath != null && csvPath.getParent() != null) Files.createDirectories(csvPath.getParent()); } catch (Exception ignored) {}
            this.io = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "db-persist"); t.setDaemon(true); return t; });
            scheduleFlush();
        } else {
            this.io = null;
        }
    }

    public long getBalance(String account) {
        rw.readLock().lock();
        try {
            return checking.getOrDefault(account, 0L);
        } finally {
            rw.readLock().unlock();
        }
    }

    public boolean transfer(String from, String to, long amount) {
        if (amount < 0) return false;
        rw.writeLock().lock();
        try {
            long fromBal = checking.getOrDefault(from, 0L);
            if (fromBal < amount) return false;
            checking.put(from, fromBal - amount);
            checking.put(to, checking.getOrDefault(to, 0L) + amount);
            boolean ok = true;
            if (persist) scheduleFlush();
            return ok;
        } finally {
            rw.writeLock().unlock();
        }
    }

    public String dump() {
        rw.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                sb.append("checking:").append(k).append("=").append(checking.getOrDefault(k, 0L)).append("\n");
            }
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                sb.append("savings:").append(k).append("=").append(savings.getOrDefault(k, 0L)).append("\n");
            }
            return sb.toString();
        } finally {
            rw.readLock().unlock();
        }
    }

    public Map<String, Long> snapshot() {
        rw.readLock().lock();
        try {
            java.util.HashMap<String, Long> out = new java.util.HashMap<>();
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                out.put("checking:" + k, checking.getOrDefault(k, 0L));
            }
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                out.put("savings:" + k, savings.getOrDefault(k, 0L));
            }
            return out;
        } finally {
            rw.readLock().unlock();
        }
    }

    public byte[] canonicalSnapshotBytes() {
        rw.readLock().lock();
        try {
            StringBuilder sb = new StringBuilder();
            boolean sbank = Boolean.getBoolean("pbft.benchmark.smallbank.enabled");
            if (sbank) {
                for (char c = 'A'; c <= 'J'; c++) {
                    String k = String.valueOf(c);
                    sb.append("checking:").append(k).append("=").append(Long.toString(checking.getOrDefault(k, 0L))).append("\n");
                }
                for (char c = 'A'; c <= 'J'; c++) {
                    String k = String.valueOf(c);
                    sb.append("savings:").append(k).append("=").append(Long.toString(savings.getOrDefault(k, 0L))).append("\n");
                }
            } else {
                for (char c = 'A'; c <= 'J'; c++) {
                    String k = String.valueOf(c);
                    sb.append(k).append("=").append(Long.toString(checking.getOrDefault(k, 0L))).append("\n");
                }
            }
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        } finally {
            rw.readLock().unlock();
        }
    }

    public void restoreFromCanonical(byte[] payload) {
        if (payload == null) return;
        rw.writeLock().lock();
        try {
            checking.clear();
            savings.clear();
            String s = new String(payload, StandardCharsets.UTF_8);
            String[] lines = s.split("\n");
            for (String line : lines) {
                if (line == null || line.isEmpty()) continue;
                int eq = line.indexOf('=');
                if (eq <= 0) continue;
                String k = line.substring(0, eq);
                long v;
                try { v = Long.parseLong(line.substring(eq + 1)); } catch (Exception ignore) { continue; }
                if (k.startsWith("checking:")) {
                    String acct = k.substring("checking:".length());
                    checking.put(acct, v);
                } else if (k.startsWith("savings:")) {
                    String acct = k.substring("savings:".length());
                    savings.put(acct, v);
                } else {
                    checking.put(k, v);
                }
            }
            if (persist) scheduleFlush();
        } finally {
            rw.writeLock().unlock();
        }
    }

    public void reset() {
        rw.writeLock().lock();
        try {
            checking.clear();
            savings.clear();
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                checking.put(k, 10L);
                savings.put(k, 0L);
            }
            if (persist) scheduleFlush();
        } finally {
            rw.writeLock().unlock();
        }
    }

    private void scheduleFlush() {
        if (!persist || io == null || csvPath == null) return;
        if (!pending.compareAndSet(false, true)) return;
        io.schedule(this::flushNow, debounceMs, TimeUnit.MILLISECONDS);
    }

    private void flushNow() {
        try {
            Map<String, Long> snap = snapshot();
            StringBuilder sb = new StringBuilder();
            sb.append("account,balance\n");
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                long v = snap.getOrDefault("checking:" + k, 0L);
                sb.append("checking:").append(k).append(",").append(Long.toString(v)).append("\n");
            }
            for (char c = 'A'; c <= 'J'; c++) {
                String k = String.valueOf(c);
                long v = snap.getOrDefault("savings:" + k, 0L);
                sb.append("savings:").append(k).append(",").append(Long.toString(v)).append("\n");
            }
            Path tmp = csvPath.resolveSibling(csvPath.getFileName().toString() + ".tmp");
            Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try { Files.move(tmp, csvPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING); } catch (Exception ignored) {}
        } catch (Exception ignored) {
        } finally {
            pending.set(false);
        }
    }

    public void close() {
        if (!persist || io == null) return;
        try { flushNow(); } catch (Exception ignored) {}
        try { io.shutdownNow(); } catch (Exception ignored) {}
    }

    public long getChecking(String account) {
        rw.readLock().lock();
        try {
            return checking.getOrDefault(account, 0L);
        } finally {
            rw.readLock().unlock();
        }
    }

    public long getSavings(String account) {
        rw.readLock().lock();
        try {
            return savings.getOrDefault(account, 0L);
        } finally {
            rw.readLock().unlock();
        }
    }

    public long getTotal(String account) {
        rw.readLock().lock();
        try {
            return checking.getOrDefault(account, 0L) + savings.getOrDefault(account, 0L);
        } finally {
            rw.readLock().unlock();
        }
    }

    public boolean depositChecking(String account, long amount) {
        if (amount < 0) return false;
        rw.writeLock().lock();
        try {
            long cur = checking.getOrDefault(account, 0L);
            checking.put(account, cur + amount);
            if (persist) scheduleFlush();
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }

    public boolean transactSavings(String account, long amount) {

        rw.writeLock().lock();
        try {
            long cur = savings.getOrDefault(account, 0L);
            long next = cur + amount;
            if (next < 0) return false;
            savings.put(account, next);
            if (persist) scheduleFlush();
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }

    public boolean sendPayment(String from, String to, long amount) {
        if (amount < 0) return false;
        rw.writeLock().lock();
        try {
            long fb = checking.getOrDefault(from, 0L);
            if (fb < amount) return false;
            checking.put(from, fb - amount);
            checking.put(to, checking.getOrDefault(to, 0L) + amount);
            if (persist) scheduleFlush();
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }

    public boolean writeCheck(String account, long amount) {
        if (amount < 0) return false;
        rw.writeLock().lock();
        try {
            long cb = checking.getOrDefault(account, 0L);
            long sb = savings.getOrDefault(account, 0L);
            long total = cb + sb;
            long debit = (total < amount) ? (amount + 1) : amount;
            long next = cb - debit;
            checking.put(account, next);
            if (persist) scheduleFlush();
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }

    public boolean amalgamate(String src, String dst) {
        rw.writeLock().lock();
        try {
            long total = checking.getOrDefault(src, 0L) + savings.getOrDefault(src, 0L);
            checking.put(src, 0L);
            savings.put(src, 0L);
            long dstCb = checking.getOrDefault(dst, 0L);
            checking.put(dst, dstCb + total);
            if (persist) scheduleFlush();
            return true;
        } finally {
            rw.writeLock().unlock();
        }
    }
}
