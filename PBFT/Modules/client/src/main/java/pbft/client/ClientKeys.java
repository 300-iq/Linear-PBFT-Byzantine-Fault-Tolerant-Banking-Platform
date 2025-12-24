package pbft.client;

import pbft.common.crypto.KeyFiles;

import java.nio.file.Path;
import java.security.PrivateKey;
import java.util.HashMap;
import java.util.Map;
import java.security.GeneralSecurityException;
import java.io.IOException;

public final class ClientKeys {
    private final Map<String, PrivateKey> keys = new HashMap<>();

    public static ClientKeys load(Path keysDir) {
        ClientKeys ks = new ClientKeys();
        for (char c = 'A'; c <= 'J'; c++) {
            String id = String.valueOf(c);
            try {
                PrivateKey sk = KeyFiles.loadPrivateKeyPem(keysDir.resolve(id).resolve("ed25519.key"));
                ks.keys.put(id, sk);
            } catch (GeneralSecurityException | IOException e) {
                throw new RuntimeException("Failed to load key for client " + id + ": " + e.getMessage(), e);
            }
        }
        return ks;
    }

    public PrivateKey get(String clientId) {
        PrivateKey k = keys.get(clientId);
        if (k == null) throw new IllegalArgumentException("No private key for client: " + clientId);
        return k;
    }
}
