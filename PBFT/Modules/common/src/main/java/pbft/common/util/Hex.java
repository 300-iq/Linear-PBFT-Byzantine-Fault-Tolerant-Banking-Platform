package pbft.common.util;

public final class Hex {
    private Hex() {}

    public static String toHexOrEmpty(byte[] data) {
        if (data == null) return "";
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    public static String toHexOrNullLiteral(byte[] data) {
        if (data == null) return "null";
        StringBuilder sb = new StringBuilder(data.length * 2);
        for (byte b : data) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    public static byte[] fromHex(String s) {
        if (s == null) return new byte[0];
        String str = s.trim();
        int len = str.length();
        if ((len & 1) == 1) throw new IllegalArgumentException("hex length must be even: " + len);
        byte[] out = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            int hi = Character.digit(str.charAt(i), 16);
            int lo = Character.digit(str.charAt(i + 1), 16);
            if (hi < 0 || lo < 0) throw new IllegalArgumentException("invalid hex character at position " + i);
            out[i / 2] = (byte) ((hi << 4) | lo);
        }
        return out;
    }
}
