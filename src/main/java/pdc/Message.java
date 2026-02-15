package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String sender;
    public long timestamp;
    public byte[] payload;

    public static void packString(String msg, OutputStream out) throws IOException{
        byte[] data = msg.getBytes("UTF-8");
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(data.length);
        dos.write(data);
        dos.flush();
    }

    public static String unpackString(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int len = dis.readInt();
        byte[] data = new byte[len];
        dis.readFully(data);
        return new String(data, "UTF-8");
    }

    public Message() {
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        // TODO: Implement custom binary or tag-based framing
        throw new UnsupportedOperationException("You must design your own wire protocol.");
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        // TODO: Implement custom parsing logic
        return null;
    }
}
