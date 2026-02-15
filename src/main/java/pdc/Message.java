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
    public String messageType;
    public String sender;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public static void packString(String msg, OutputStream out) throws IOException {
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

    public static void packMatrix(int[][] matrix, OutputStream outputStream) throws IOException {
        DataOutputStream dos = new DataOutputStream(outputStream);
        int rows = matrix.length;
        int cols = matrix[0].length;
        dos.writeInt(rows);
        dos.writeInt(cols);
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                dos.writeInt(matrix[i][j]);
        dos.flush();
    }

    public static int[][] unpackMatrix(InputStream inputStream) throws IOException {
        DataInputStream dis = new DataInputStream(inputStream);
        int rows = dis.readInt();
        int cols = dis.readInt();
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++)
            for (int j = 0; j < cols; j++)
                matrix[i][j] = dis.readInt();
        return matrix;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            byte[] magicBytes = magic.getBytes("UTF-8");
            byte[] messageTypeBytes = messageType.getBytes("UTF-8");
            byte[] senderBytes = sender.getBytes("UTF-8");
            byte[] studentIdBytes = (studentId != null ? studentId.getBytes("UTF-8") : new byte[0]);
            int payloadLength = (payload != null) ? payload.length : 0;

            int totalLength = 4 + magicBytes.length + 4 + messageTypeBytes.length + 4 + senderBytes.length + 4
                    + studentIdBytes.length + 8 + 4 + payloadLength + 4;

            java.io.ByteArrayOutputStream byteArrayOutputStream = new java.io.ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
            dataOutputStream.writeInt(totalLength); // length prefix (excluding itself)

            dataOutputStream.writeInt(magicBytes.length);
            dataOutputStream.write(magicBytes);
            dataOutputStream.writeInt(version);
            dataOutputStream.writeInt(messageTypeBytes.length);
            dataOutputStream.write(messageTypeBytes);
            dataOutputStream.writeInt(senderBytes.length);
            dataOutputStream.write(senderBytes);
            dataOutputStream.writeInt(studentIdBytes.length);
            dataOutputStream.write(studentIdBytes);
            dataOutputStream.writeLong(timestamp);
            dataOutputStream.writeInt(payloadLength);
            if (payloadLength > 0)
                dataOutputStream.write(payload);
            dataOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Message pack error", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            DataInputStream dataInputStream = new DataInputStream(new java.io.ByteArrayInputStream(data));
            int totalLength = dataInputStream.readInt();
            int magicLength = dataInputStream.readInt();
            byte[] magicBytes = new byte[magicLength];
            dataInputStream.readFully(magicBytes);
            String magic = new String(magicBytes, "UTF-8");
            int version = dataInputStream.readInt();
            int messageTypeLength = dataInputStream.readInt();
            byte[] messageTypeBytes = new byte[messageTypeLength];
            dataInputStream.readFully(messageTypeBytes);
            String messageType = new String(messageTypeBytes, "UTF-8");
            int senderLength = dataInputStream.readInt();
            byte[] senderBytes = new byte[senderLength];
            dataInputStream.readFully(senderBytes);
            String sender = new String(senderBytes, "UTF-8");
            int studentIdLength = dataInputStream.readInt();
            byte[] studentIdBytes = new byte[studentIdLength];
            dataInputStream.readFully(studentIdBytes);
            String studentId = new String(studentIdBytes, "UTF-8");
            long timestamp = dataInputStream.readLong();
            int payloadLength = dataInputStream.readInt();
            byte[] payload = new byte[payloadLength];
            if (payloadLength > 0)
                dataInputStream.readFully(payload);
            Message message = new Message();
            message.magic = magic;
            message.version = version;
            message.messageType = messageType;
            message.sender = sender;
            message.studentId = studentId;
            message.timestamp = timestamp;
            message.payload = payload;
            return message;
        } catch (IOException e) {
            throw new RuntimeException("Message unpack error", e);
        }
    }
}
