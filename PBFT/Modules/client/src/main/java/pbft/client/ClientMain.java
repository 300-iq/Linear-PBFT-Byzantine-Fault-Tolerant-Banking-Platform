package pbft.client;

import pbft.proto.*;
import pbft.proto.BalanceEntry;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.nio.file.Path;
import java.util.function.Function;

import pbft.common.config.ClusterConfig;
import pbft.common.config.KeyStore;
import pbft.common.validation.MessageDecoder;
import pbft.common.util.Hex;
import pbft.client.bench.BenchmarkRunner;

public class ClientMain implements Callable<Integer> {
    static {
        if (System.getProperty("logback.statusListenerClass") == null) {
            System.setProperty("logback.statusListenerClass", "ch.qos.logback.core.status.NopStatusListener");
        }
    }

    private static void printCompleteLogTable(java.util.List<CompleteLogEntry> entries) {
        if (entries.isEmpty()) { System.out.println("(empty)"); return; }
        Function<Long,String> fmtTs = ts -> {
            try {
                var inst = java.time.Instant.ofEpochMilli(ts);
                var zdt = java.time.ZonedDateTime.ofInstant(inst, java.time.ZoneId.systemDefault());
                var fmt = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                return zdt.format(fmt);
            } catch (Exception e) { return Long.toString(ts); }
        };
        int wTs = Math.max("Time".length(), entries.stream().map(e -> fmtTs.apply(e.getTsMillis()).length()).max(Comparator.naturalOrder()).orElse(0));
        int wDir = Math.max("Dir".length(), entries.stream().map(e -> e.getDirection().length()).max(Comparator.naturalOrder()).orElse(0));
        int wType = Math.max("Type".length(), entries.stream().map(e -> e.getType().length()).max(Comparator.naturalOrder()).orElse(0));
        int wV = Math.max("V".length(), entries.stream().map(e -> Long.toString(e.getView()).length()).max(Comparator.naturalOrder()).orElse(0));
        int wS = Math.max("S".length(), entries.stream().map(e -> Long.toString(e.getSeq()).length()).max(Comparator.naturalOrder()).orElse(0));
        int wFrom = Math.max("From".length(), entries.stream().map(e -> e.getFrom().length()).max(Comparator.naturalOrder()).orElse(0));
        int wTo = Math.max("To".length(), entries.stream().map(e -> e.getTo().length()).max(Comparator.naturalOrder()).orElse(0));
        int wClient = Math.max("Client".length(), entries.stream().map(e -> e.getClientId().length()).max(Comparator.naturalOrder()).orElse(0));
        int wT = Math.max("t".length(), entries.stream().map(e -> Long.toString(e.getReqSeq()).length()).max(Comparator.naturalOrder()).orElse(0));
        java.util.function.Function<String,String> shortDg = dg -> {
            if (dg == null) return ""; String d = dg; return d.length() > 8 ? d.substring(0, 8) : d;
        };
        int wDg = Math.max("Digest".length(), entries.stream().map(e -> shortDg.apply(e.getRequestDigest())).max(Comparator.comparingInt(String::length)).orElse("").length());
        int wStatus = Math.max("Status".length(), entries.stream().map(e -> e.getStatus().length()).max(Comparator.naturalOrder()).orElse(0));
        int wNotes = Math.max("Notes".length(), entries.stream().map(e -> e.getNotes().length()).max(Comparator.naturalOrder()).orElse(0));

        String sep = "+" + "-".repeat(wTs+2) + "+" + "-".repeat(wDir+2) + "+" + "-".repeat(wType+2) + "+" + "-".repeat(wV+2) + "+" + "-".repeat(wS+2) + "+" + "-".repeat(wFrom+2) + "+" + "-".repeat(wTo+2) + "+" + "-".repeat(wClient+2) + "+" + "-".repeat(wT+2) + "+" + "-".repeat(wDg+2) + "+" + "-".repeat(wStatus+2) + "+" + "-".repeat(wNotes+2) + "+";
        String hdr = String.format("| %1$-" + wTs + "s | %2$-" + wDir + "s | %3$-" + wType + "s | %4$-" + wV + "s | %5$-" + wS + "s | %6$-" + wFrom + "s | %7$-" + wTo + "s | %8$-" + wClient + "s | %9$-" + wT + "s | %10$-" + wDg + "s | %11$-" + wStatus + "s | %12$-" + wNotes + "s |",
                "Time","Dir","Type","V","S","From","To","Client","t","Digest","Status","Notes");
        System.out.println(sep);
        System.out.println(hdr);
        System.out.println(sep);
        for (var e : entries) {
            String row = String.format("| %1$-" + wTs + "s | %2$-" + wDir + "s | %3$-" + wType + "s | %4$-" + wV + "s | %5$-" + wS + "s | %6$-" + wFrom + "s | %7$-" + wTo + "s | %8$-" + wClient + "s | %9$-" + wT + "s | %10$-" + wDg + "s | %11$-" + wStatus + "s | %12$-" + wNotes + "s |",
                    fmtTs.apply(e.getTsMillis()), e.getDirection(), e.getType(), Long.toString(e.getView()), Long.toString(e.getSeq()), e.getFrom(), e.getTo(), e.getClientId(), Long.toString(e.getReqSeq()), shortDg.apply(e.getRequestDigest()), e.getStatus(), e.getNotes());
            System.out.println(row);
        }
        System.out.println(sep);
    }

    private static void printCompleteLogTableDedup(java.util.List<CompleteLogEntry> entries) {
        if (entries.isEmpty()) { System.out.println("(empty)"); return; }
        java.util.LinkedHashMap<String, CompleteLogEntry> unique = new java.util.LinkedHashMap<>();
        for (var e : entries) {
            String dg = e.getRequestDigest();
            String key = String.join("|",
                    e.getDirection(), e.getType(), Long.toString(e.getView()), Long.toString(e.getSeq()),
                    e.getFrom(), e.getTo(), e.getClientId(), Long.toString(e.getReqSeq()), dg == null ? "" : dg,
                    e.getStatus(), e.getNotes());
            unique.putIfAbsent(key, e);
        }
        printCompleteLogTable(new java.util.ArrayList<>(unique.values()));
    }

    private static void printStateEntriesTable(java.util.List<LogEntry> entries) {
        if (entries.isEmpty()) { System.out.println("(empty)"); return; }
        java.util.function.Function<String,String> shortDg = dg -> {
            if (dg == null) return ""; String d = dg; return d.length() > 8 ? d.substring(0, 8) : d;
        };
        int wV = Math.max("V".length(), entries.stream().map(e -> Long.toString(e.getView()).length()).max(Comparator.naturalOrder()).orElse(0));
        int wS = Math.max("S".length(), entries.stream().map(e -> Long.toString(e.getSeq()).length()).max(Comparator.naturalOrder()).orElse(0));
        int wClient = Math.max("Client".length(), entries.stream().map(e -> e.getClientId().length()).max(Comparator.naturalOrder()).orElse(0));
        int wT = Math.max("t".length(), entries.stream().map(e -> Long.toString(e.getReqSeq()).length()).max(Comparator.naturalOrder()).orElse(0));
        int wDg = Math.max("Digest".length(), entries.stream().map(e -> shortDg.apply(e.getRequestDigest())).max(Comparator.comparingInt(String::length)).orElse("").length());
        int wStatus = Math.max("Status".length(), entries.stream().map(e -> e.getStatus().length()).max(Comparator.naturalOrder()).orElse(0));
        String sep = "+" + "-".repeat(wV+2) + "+" + "-".repeat(wS+2) + "+" + "-".repeat(wClient+2) + "+" + "-".repeat(wT+2) + "+" + "-".repeat(wDg+2) + "+" + "-".repeat(wStatus+2) + "+-----+-----+-----+-----+";
        String hdr = String.format("| %1$-" + wV + "s | %2$-" + wS + "s | %3$-" + wClient + "s | %4$-" + wT + "s | %5$-" + wDg + "s | %6$-" + wStatus + "s |  PP |  P  |  C  |  E  |",
                "V","S","Client","t","Digest","Status");
        System.out.println(sep);
        System.out.println(hdr);
        System.out.println(sep);
        for (var e : entries) {
            String row = String.format("| %1$-" + wV + "s | %2$-" + wS + "s | %3$-" + wClient + "s | %4$-" + wT + "s | %5$-" + wDg + "s | %6$-" + wStatus + "s |  %7$s  |  %8$s  |  %9$s  |  %10$s  |",
                    Long.toString(e.getView()), Long.toString(e.getSeq()), e.getClientId(), Long.toString(e.getReqSeq()), shortDg.apply(e.getRequestDigest()), e.getStatus(),
                    e.getPrePrepared() ? "Y" : "-", e.getPrepared() ? "Y" : "-", e.getCommitted() ? "Y" : "-", e.getExecuted() ? "Y" : "-");
            System.out.println(row);
        }
        System.out.println(sep);
    }
    private static final Logger log = LoggerFactory.getLogger(ClientMain.class);

    @CommandLine.Option(names="--config", required=true) Path configPath;
    @CommandLine.Option(names="--keys-dir", required=true) Path keysDir;
    @CommandLine.Option(names="--csv", required=false) Path csvPath;
    @CommandLine.Option(names="--cmd", required=false, defaultValue = "run", description = "run|printdb|printlog|printview|printstatus|printcheckpoint|printcompletelog|bench|benchshell") String cmd;
    @CommandLine.Option(names="--replica", required=false, defaultValue = "n1", description = "replica id for admin prints") String replicaId;
    @CommandLine.Option(names="--raw", required=false, defaultValue = "false", description = "for printcompletelog: show raw event stream") boolean rawCompleteLog;
    @CommandLine.Option(names="--view", required=false, defaultValue = "1") long view;
    @CommandLine.Option(names="--seq", required=false, defaultValue = "1") long seq;

    @CommandLine.Option(names="--bench-warmup", required=false, defaultValue = "5") long benchWarmupSec;
    @CommandLine.Option(names="--bench-duration", required=false, defaultValue = "30") long benchDurationSec;
    @CommandLine.Option(names="--bench-xfer-pct", required=false, defaultValue = "50") int benchTransferPct;
    @CommandLine.Option(names="--bench-seed", required=false, defaultValue = "1") long benchSeed;
    @CommandLine.Option(names="--bench-hot-prob", required=false, defaultValue = "0.80") double benchHotProb;
    @CommandLine.Option(names="--bench-hot-k", required=false, defaultValue = "2") int benchHotK;
    @CommandLine.Option(names="--bench-max-ops", required=false, defaultValue = "0") long benchMaxOps;
    @CommandLine.Option(names="--enable-bench", required=false, defaultValue = "false", description = "set pbft.benchmark.enabled=true at runtime") boolean enableBench;

    public static void main(String[] args) {
        System.exit(new CommandLine(new ClientMain()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        if (cmd.equalsIgnoreCase("run")) {
            if (csvPath == null) {
                log.error("--csv is required for cmd=run");
                return 2;
            }
            try (ClientController runner = new ClientController(configPath, keysDir)) {
                runner.runCsv(csvPath);
            }
            return 0;
        }
        if (cmd.equalsIgnoreCase("bench")) {
            if (enableBench) {
                System.setProperty("pbft.benchmark.enabled", "true");
            }
            if (!Boolean.getBoolean("pbft.benchmark.enabled")) {
                log.error("Benchmarking not enabled. Set -Dpbft.benchmark.enabled=true or pass --enable-bench.");
                return 2;
            }
            if (System.getProperty("pbft.client.quiet") == null) {
                System.setProperty("pbft.client.quiet", "true");
            }
            try (BenchmarkRunner br = new BenchmarkRunner(configPath, keysDir)) {
                BenchmarkRunner.Config cfg = new BenchmarkRunner.Config(
                        configPath,
                        keysDir,
                        benchWarmupSec,
                        benchDurationSec,
                        Math.max(0, Math.min(100, benchTransferPct)),
                        benchSeed,
                        benchHotProb,
                        benchHotK,
                        benchMaxOps
                );
                br.run(cfg);
            }
            return 0;
        }
        if (cmd.equalsIgnoreCase("benchshell")) {
            if (enableBench) {
                System.setProperty("pbft.benchmark.enabled", "true");
            }
            if (!Boolean.getBoolean("pbft.benchmark.enabled")) {
                log.error("Benchmarking not enabled. Set -Dpbft.benchmark.enabled=true or pass --enable-bench.");
                return 2;
            }
            if (System.getProperty("pbft.client.quiet") == null) {
                System.setProperty("pbft.client.quiet", "true");
            }
            try (BenchmarkRunner br = new BenchmarkRunner(configPath, keysDir)) {
                BenchmarkRunner.Config cfg = new BenchmarkRunner.Config(
                        configPath,
                        keysDir,
                        benchWarmupSec,
                        benchDurationSec,
                        Math.max(0, Math.min(100, benchTransferPct)),
                        benchSeed,
                        benchHotProb,
                        benchHotK,
                        benchMaxOps
                );
                br.runInteractiveCLI(cfg);
            }
            return 0;
        }

        ClusterConfig cfg = ClusterConfig.load(configPath);
        java.nio.file.Path baseDir = configPath.toAbsolutePath().getParent().getParent();
        KeyStore keyStore = KeyStore.from(cfg, baseDir);
        var rep = cfg.replicas.stream().filter(r -> r.id.equals(replicaId)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown replica id: " + replicaId));
        ManagedChannel ch = ManagedChannelBuilder.forAddress(rep.host, rep.port).usePlaintext().build();
        AdminServiceGrpc.AdminServiceBlockingStub admin = AdminServiceGrpc.newBlockingStub(ch);

        switch (cmd.toLowerCase()) {
            case "printdb" -> {
                PrintDBReply r = admin.printDB(Empty.getDefaultInstance());
                System.out.println("DB:");
                printBalanceTable(r.getBalancesList());
            }
            case "printlog" -> {
                PrintLogReply r = admin.printLog(Empty.getDefaultInstance());
                System.out.println("LOG:");
                for (var e : r.getEntriesList()) {
                    String phases = String.format("PP=%s P=%s C=%s E=%s",
                            e.getPrePrepared(), e.getPrepared(), e.getCommitted(), e.getExecuted());
                    System.out.println("(v=" + e.getView() + ", s=" + e.getSeq() + ") "
                            + "client=" + e.getClientId() + "#" + e.getReqSeq()
                            + " status=" + e.getStatus()
                            + " digest=" + e.getRequestDigest()
                            + " op=" + e.getOperation()
                            + " phases[" + phases + "]");
                }
            }
            case "printcompletelog" -> {
                PrintCompleteLogReply r = admin.printCompleteLog(Empty.getDefaultInstance());
                if (rawCompleteLog) {
                    System.out.println("Node " + replicaId + " COMPLETE LOG (raw events):");
                    printCompleteLogTable(r.getEntriesList());
                } else {
                    System.out.println("Node " + replicaId + " COMPLETE LOG:");
                    printCompleteLogTableDedup(r.getEntriesList());
                }
            }
            case "printcheckpoint" -> {
                PrintCheckpointReply r = admin.printCheckpoint(Empty.getDefaultInstance());
                String dg = Hex.toHexOrEmpty(r.getLastStableDigest().toByteArray());
                if (dg.length() > 8) dg = dg.substring(0, 8);
                System.out.println("CHECKPOINT:");
                System.out.println("enabled=" + r.getEnabled());
                System.out.println("period=" + r.getPeriod());
                System.out.println("last_stable_seq=" + r.getLastStableSeq());
                System.out.println("last_stable_digest=" + dg);
                System.out.println("stable_signers=" + r.getStableSigners());
                if (r.getStableSignerIdsCount() > 0) {
                    System.out.println("stable_signer_ids=" + r.getStableSignerIdsList());
                }
                System.out.println("has_local_snapshot=" + r.getHasLocalSnapshot());
            }
            case "printview" -> {
                ViewLog r = admin.printView(Empty.getDefaultInstance());
                System.out.println("VIEW MESSAGES:");
                for (var s : r.getNewViewsList()) System.out.println(s);
                if (r.getHasLast()) {
                    System.out.println("Last NewView v'=" + r.getLastView() + " s=" + r.getS() + " h=" + r.getH());
                    printNewView(r.getLastNewView(), "  ", keyStore);
                }
                if (!r.getNewViewsFullList().isEmpty()) {
                    System.out.println("Full NewView history:");
                    var nvs = r.getNewViewsFullList();
                    var sCov = r.getSCovList();
                    var hCov = r.getHCovList();
                    for (int i = 0; i < nvs.size(); i++) {
                        long sVal = (i < sCov.size()) ? sCov.get(i) : 0L;
                        long hVal = (i < hCov.size()) ? hCov.get(i) : 0L;
                        System.out.println("  v'=" + nvs.get(i).getNewView() + " s=" + sVal + " h=" + hVal);
                        printNewView(nvs.get(i), "    ", keyStore);
                    }
                }
            }
            case "printstatus" -> {
                StatusReply r = admin.printStatus(
                        SeqQuery.newBuilder().setView(view).setSeq(seq).setAggregate(true).build());
                System.out.println("STATUS(v=" + view + ", s=" + seq + ") across replicas:");
                for (var s : r.getStatusesList()) {
                    System.out.println(s.getReplicaId() + ": " + s.getStatus());
                }
            }
            default -> throw new IllegalArgumentException("Unknown --cmd: " + cmd);
        }
        ch.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
        return 0;
    }

    private static void printBalanceTable(java.util.List<BalanceEntry> balances) {
        if (balances.isEmpty()) {
            System.out.println("(empty)");
            return;
        }
        int accountWidth = Math.max("Account".length(),
                balances.stream().map(b -> b.getAccount().length()).max(Comparator.naturalOrder()).orElse(0));
        int balanceWidth = Math.max("Balance".length(),
                balances.stream().map(b -> Long.toString(b.getBalance()).length()).max(Comparator.naturalOrder()).orElse(0));

        String horizontal = "+" + "-".repeat(accountWidth + 2) + "+" + "-".repeat(balanceWidth + 2) + "+";
        String header = String.format("| %-" + accountWidth + "s | %-" + balanceWidth + "s |", "Account", "Balance");

        System.out.println(horizontal);
        System.out.println(header);
        System.out.println(horizontal);
        for (var b : balances) {
            String row = String.format("| %-" + accountWidth + "s | %-" + balanceWidth + "s |",
                    b.getAccount(), Long.toString(b.getBalance()));
            System.out.println(row);
        }
        System.out.println(horizontal);
    }

    private static void printNewView(NewView nv, String indent, KeyStore keyStore) {
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
}

