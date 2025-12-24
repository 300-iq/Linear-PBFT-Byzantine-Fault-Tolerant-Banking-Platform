package pbft.common.validation;

public final class PbftMsgTypes {
    private PbftMsgTypes() {}

    public static final String CLIENT_REQUEST = "pbft.ClientRequest";

    public static final String PRE_PREPARE    = "pbft.PrePrepare";
    public static final String PREPARE        = "pbft.Prepare";
    public static final String COMMIT         = "pbft.Commit";
    public static final String CLIENT_REPLY   = "pbft.ClientReply";

    public static final String PREPARE_PROOF  = "pbft.PrepareProof";
    public static final String COMMIT_PROOF   = "pbft.CommitProof";

    public static final String VIEW_CHANGE    = "pbft.ViewChange";
    public static final String NEW_VIEW       = "pbft.NewView";
    public static final String CHECKPOINT     = "pbft.Checkpoint";
}

