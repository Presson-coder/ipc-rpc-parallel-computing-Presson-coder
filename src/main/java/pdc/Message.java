package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Message {

    public static final String CSM218_MAGIC = "CSM218";

    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public int messageType;
    public String studentId;

    public Message() {}

    // --------- Efficient ByteBuffer pack/unpack ----------

    public byte[] pack() {
        if (magic == null || magic.isBlank()) magic = CSM218_MAGIC;
        if (version == 0) version = 1; // default version
        if (studentId == null || studentId.isBlank()) studentId = resolveStudentId();
        if (payload == null) payload = new byte[0];

        byte[] magicB = magic.getBytes(StandardCharsets.UTF_8);
        byte[] studentB = studentId.getBytes(StandardCharsets.UTF_8);
        byte[] typeB = (type == null ? "" : type).getBytes(StandardCharsets.UTF_8);
        byte[] senderB = (sender == null ? "" : sender).getBytes(StandardCharsets.UTF_8);
        byte[] payloadB = payload;

        int bodyLen =
                4 + magicB.length +
                4 +                 // version
                4 +                 // messageType
                4 + studentB.length +
                4 + typeB.length +
                4 + senderB.length +
                8 +                 // timestamp
                4 + payloadB.length;

        ByteBuffer buf = ByteBuffer.allocate(4 + bodyLen);
        buf.putInt(bodyLen);

        putBytes(buf, magicB);
        buf.putInt(version);
        buf.putInt(messageType);
        putBytes(buf, studentB);
        putBytes(buf, typeB);
        putBytes(buf, senderB);
        buf.putLong(timestamp);
        putBytes(buf, payloadB);

        return buf.array();
    }

    public static Message unpack(byte[] data) {
        if (data == null || data.length < 4) return null;

        ByteBuffer buf = ByteBuffer.wrap(data);
        int frameLen = buf.getInt();
        if (frameLen < 0 || data.length < 4 + frameLen) return null;

        Message m = new Message();
        m.magic = getString(buf);
        if (!CSM218_MAGIC.equals(m.magic)) return null; // validate magic

        m.version = buf.getInt();
        m.messageType = buf.getInt();
        m.studentId = getString(buf);
        m.type = getString(buf);
        m.sender = getString(buf);
        m.timestamp = buf.getLong();
        m.payload = getBytes(buf);
        return m;
    }

    private static void putBytes(ByteBuffer buf, byte[] b) {
        buf.putInt(b.length);
        buf.put(b);
    }

    private static byte[] getBytes(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0 || len > buf.remaining()) throw new IllegalArgumentException("Bad length");
        byte[] out = new byte[len];
        buf.get(out);
        return out;
    }

    private static String getString(ByteBuffer buf) {
        return new String(getBytes(buf), StandardCharsets.UTF_8);
    }

    // --------- TCP stream safe helpers ----------

    public static Message readFrom(DataInputStream in) throws IOException {
        try {
            int frameLen = in.readInt();
            if (frameLen < 0) throw new IOException("Negative frame length");

            byte[] frame = new byte[4 + frameLen];
            ByteBuffer.wrap(frame).putInt(frameLen);
            
            // Explicit loop for fragmentation handling (supports jumbo payloads > MTU)
            int bytesRead = 0;
            while (bytesRead < frameLen) {
                int n = in.read(frame, 4 + bytesRead, frameLen - bytesRead);
                if (n == -1) throw new EOFException("Connection closed during frame read");
                bytesRead += n;
            }

            return unpack(frame);
        } catch (EOFException eof) {
            return null;
        }
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.write(pack());
        out.flush();
    }

    private static String resolveStudentId() {
        String v = System.getenv("CSM218_STUDENT_ID");
        if (v == null || v.isBlank()) v = System.getenv("STUDENT_ID");
        if (v == null || v.isBlank()) v = System.getenv("STUDENT_NO");
        if (v == null || v.isBlank()) v = System.getenv("GITHUB_USERNAME");
        return (v == null || v.isBlank()) ? "UNKNOWN" : v.trim();
    }
}

