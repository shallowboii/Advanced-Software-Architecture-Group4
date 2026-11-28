import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/** Binary format of kafka messages **/
final class Wire {
    static final int SIZE = 1040;
    static final int O_TYPE = 0, O_MTYPE = 4, O_MID = 36, O_BATCH = 40, O_MSGID = 104,
                     O_TS = 200, O_PARAMS = 232, O_NPARAMS = 872, O_ECODE = 880, O_EREASON = 912;
    static final int L_NAME = 32, L_BATCH = 64, L_MSGID = 96, L_TS = 32, L_REASON = 128;
    static final int PARAM_SIZE = 40;   // name[32] + double
    static final int MAX_PARAMS = 16;
    static final String[] TYPES = {"START", "STOP", "FLUSH", "IDLE", "RUNNING", "ERROR"};

    record Msg(String type, String machineType, int machineId, String batchId,
               String errorCode, String errorReason) {}

    private static void putStr(ByteBuffer b, int off, int cap, String s) {
        byte[] x = s.getBytes(StandardCharsets.US_ASCII);
        b.put(off, x, 0, Math.min(x.length, cap - 1));   // rest stays 0 = '\0'
    }

    private static String getStr(ByteBuffer b, int off, int cap) {
        int len = 0;
        while (len < cap - 1 && b.get(off + len) != 0) len++;
        byte[] x = new byte[len];
        b.get(off, x);
        return new String(x, StandardCharsets.US_ASCII);
    }

    static byte[] encodeStart(String machineType, int machineId, String batchId,
                              String messageId, String paramName, double paramValue) {
        ByteBuffer b = ByteBuffer.allocate(SIZE).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(O_TYPE, 0);                                   // KAFKA_START
        putStr(b, O_MTYPE, L_NAME, machineType);
        b.putInt(O_MID, machineId);
        putStr(b, O_BATCH, L_BATCH, batchId);
        putStr(b, O_MSGID, L_MSGID, messageId);
        putStr(b, O_TS, L_TS, Instant.now().truncatedTo(ChronoUnit.SECONDS).toString());
        putStr(b, O_PARAMS, L_NAME, paramName);
        b.putDouble(O_PARAMS + L_NAME, paramValue);
        b.putLong(O_NPARAMS, 1);
        return b.array();
    }

    /** Returns null if the payload is not a kafka_message_t. */
    static Msg decode(byte[] p) {
        if (p == null || p.length != SIZE) {
            System.err.printf("Kafka: payload of %d bytes ignored (expected %d)%n",
                    p == null ? -1 : p.length, SIZE);
            return null;
        }
        ByteBuffer b = ByteBuffer.wrap(p).order(ByteOrder.LITTLE_ENDIAN);
        int t = b.getInt(O_TYPE);
        if (t < 0 || t >= TYPES.length) return null;
        return new Msg(TYPES[t], getStr(b, O_MTYPE, L_NAME), b.getInt(O_MID),
                getStr(b, O_BATCH, L_BATCH), getStr(b, O_ECODE, L_NAME),
                getStr(b, O_EREASON, L_REASON));
    }
}