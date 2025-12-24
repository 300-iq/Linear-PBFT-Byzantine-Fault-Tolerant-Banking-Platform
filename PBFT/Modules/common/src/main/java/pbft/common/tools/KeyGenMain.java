package pbft.common.tools;

import pbft.common.crypto.KeyFiles;
import picocli.CommandLine;

import java.nio.file.Path;
import java.security.KeyPair;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "keygen", mixinStandardHelpOptions = true,
        description = "Generate Ed25519 keypairs for replicas/clients.")
public class KeyGenMain implements Callable<Integer> {

    @CommandLine.Option(names = "--out", description = "Output directory (e.g., secrets/replicas/n1)", required = true)
    Path outDir;

    @CommandLine.Option(names = "--id", description = "Node id (e.g., n1). Only used for print/log", required = false)
    String id;

    @CommandLine.Option(names = "--batch-replicas", description = "If set, generate n1..n7 under secrets/replicas/", required = false)
    boolean batchReplicas;

    @Override public Integer call() throws Exception {
        if (batchReplicas) {
            for (int i = 1; i <= 7; i++) {
                String nid = "n" + i;
                Path base = outDir.resolve("replicas").resolve(nid);
                KeyPair kp = KeyFiles.generateKeyPair();
                KeyFiles.writeKeyPair(base.resolve("ed25519.key"), base.resolve("ed25519.pub"), kp);
                System.out.println("Wrote " + nid + " keys to " + base);
            }
            return 0;
        }

        Path priv = outDir.resolve("ed25519.key");
        Path pub  = outDir.resolve("ed25519.pub");
        KeyPair kp = KeyFiles.generateKeyPair();
        KeyFiles.writeKeyPair(priv, pub, kp);
        System.out.println("Wrote keys for " + (id != null ? id : "(no-id)") + " to " + outDir);
        return 0;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new KeyGenMain()).execute(args));
    }
}
