package pbft.replica.rpc;

import pbft.common.config.KeyStore;

import java.security.PrivateKey;

public final class RpcSupport {
    private RpcSupport() {}

    public static final class NodeInfo {
        public final String nodeId;
        public final int n;
        public final int f;
        public final long initialView;
        public final PrivateKey sk;
        public final KeyStore keyStore;

        public NodeInfo(String nodeId, int n, int f, long initialView, PrivateKey sk, KeyStore keyStore) {
            this.nodeId = nodeId;
            this.n = n;
            this.f = f;
            this.initialView = initialView;
            this.sk = sk;
            this.keyStore = keyStore;
        }
    }

    public static final class ViewUtil {
        public static String primaryId(long view, int n) {
            if (n <= 0) return "n1";
            long idx = ((view - 1) % n) + 1; // 1..n
            return "n" + idx;
        }
    }
}
