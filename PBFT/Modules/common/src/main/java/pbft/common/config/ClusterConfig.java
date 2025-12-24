package pbft.common.config;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterConfig {
    public int n;
    public int f;
    public long initialView;
    public List<Replica> replicas;
    public List<Client> clients;

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Replica {
        public String id;
        public String host;
        public int port;
        @JsonAlias({"pubkeyPemPath", "publicKeyPath"})
        public String publicKeyPath;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Client {
        public String id;
        public String host;
        public int port;
        @JsonAlias({"pubkeyPemPath", "publicKeyPath"})
        public String publicKeyPath; 
    }

    public static ClusterConfig load(Path path) throws IOException {
        return new ObjectMapper().readValue(path.toFile(), ClusterConfig.class);
    }
}
