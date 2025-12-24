package pbft.common.validation;

import io.grpc.Status;

public final class GrpcStatusUtil {
    private GrpcStatusUtil() {}

    public static Status mapStatus(MessageValidation.Result r) {
        return switch (r.code()) {
            case OK -> Status.OK;
            case UNKNOWN_SIGNER, BAD_SIGNATURE -> Status.PERMISSION_DENIED.withDescription(r.reason());
            case BAD_TYPEURL, EMPTY_PAYLOAD, BAD_VIEW_SEQ -> Status.INVALID_ARGUMENT.withDescription(r.reason());
        };
    }
}
