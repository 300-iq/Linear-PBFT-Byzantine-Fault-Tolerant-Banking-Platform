package pbft.common.config;

import pbft.common.crypto.KeyFiles;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyStore {

    private final Map<String, PublicKey> replicas = new HashMap<>();
    private final Map<String, PublicKey> clients  = new HashMap<>();

    public static KeyStore from(ClusterConfig config, Path path) throws IOException, GeneralSecurityException {
        KeyStore keyStore = new KeyStore();

        if (config.replicas != null) {
            config.replicas.forEach(replica ->  {
                if (replica.publicKeyPath != null && !replica.publicKeyPath.isBlank()) {
                    PublicKey pk;
                    try {
                        pk = KeyFiles.loadPublicKeyPem(path.resolve(replica.publicKeyPath));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    keyStore.replicas.put(replica.id, pk);
                }
            });
        }

        if (config.clients != null) {
            config.clients.forEach(client ->  {
                if (client.publicKeyPath != null && !client.publicKeyPath.isBlank()) {
                    PublicKey pk;
                    try {
                        pk = KeyFiles.loadPublicKeyPem(path.resolve(client.publicKeyPath));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    keyStore.clients.put(client.id, pk);
                }
            });
        }
        return keyStore;
    }

    public Optional<PublicKey> replicaKey(String id) { return Optional.ofNullable(replicas.get(id)); }
    public Optional<PublicKey> clientKey(String id)  { return Optional.ofNullable(clients.get(id)); }

    public int replicaCount() { return replicas.size(); }
    public int clientCount()  { return clients.size(); }
}
