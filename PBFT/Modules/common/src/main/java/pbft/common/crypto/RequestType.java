package pbft.common.crypto;

public final class RequestType {
    private RequestType() {}

    public static final String CLIENT_REQUEST = "PBFT:CLIENT-REQUEST";
    public static final String CLIENT_REPLY   = "PBFT:CLIENT-REPLY";

    public static final String PRE_PREPARE = "PBFT:PRE-PREPARE";
    public static final String PREPARE     = "PBFT:PREPARE";
    public static final String COMMIT      = "PBFT:COMMIT";
    public static final String PREPARE_PROOF = "PBFT:PREPARE-PROOF";
    public static final String COMMIT_PROOF  = "PBFT:COMMIT-PROOF";

    public static final String VIEW_CHANGE = "PBFT:VIEW-CHANGE";
    public static final String NEW_VIEW    = "PBFT:NEW-VIEW";
    public static final String CHECKPOINT  = "PBFT:CHECKPOINT";
}
