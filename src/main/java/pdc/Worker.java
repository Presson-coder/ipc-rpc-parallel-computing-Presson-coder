package pdc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    private void sendMessage(Message message, OutputStream outputStream) throws IOException {
        byte[] messageBytes = message.pack();
        outputStream.write(messageBytes);
        outputStream.flush();
    }

    private Message receiveMessage(InputStream inputStream) throws IOException {
        byte[] lengthBytes = new byte[4];
        int read = inputStream.read(lengthBytes);
        if (read != 4)
            return null;
        int messageLength = java.nio.ByteBuffer.wrap(lengthBytes).getInt();
        byte[] messageBytes = new byte[messageLength];
        int totalRead = 0;
        while (totalRead < messageLength) {
            int r = inputStream.read(messageBytes, totalRead, messageLength - totalRead);
            if (r == -1)
                break;
            totalRead += r;
        }
        if (totalRead != messageLength)
            return null;
        return Message.unpack(
                java.nio.ByteBuffer.allocate(4 + messageLength).putInt(messageLength).put(messageBytes).array());
    }

    // Heartbeat thread for sending periodic heartbeats to the master
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    public void startHeartbeat(String masterHost, int port, String workerId) {
        Runnable heartbeatTask = () -> {
            try {
                Message heartbeatMessage = new Message();
                heartbeatMessage.magic = "CSM218";
                heartbeatMessage.version = 1;
                heartbeatMessage.messageType = "HEARTBEAT";
                heartbeatMessage.sender = workerId;
                heartbeatMessage.timestamp = System.currentTimeMillis();
                heartbeatMessage.payload = "ALIVE".getBytes("UTF-8");
                heartbeatMessage.studentId = System.getenv("STUDENT_ID");
                try (Socket socket = new Socket(masterHost, port)) {
                    OutputStream outputStream = socket.getOutputStream();
                    sendMessage(heartbeatMessage, outputStream);
                }
            } catch (Exception exception) {
                System.err.println("Failed to send heartbeat: " + exception.getMessage());
            }
        };
        heartbeatExecutor.scheduleAtFixedRate(heartbeatTask, 0, 2, java.util.concurrent.TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws IOException {
        int port = 9000;
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Worker started, listening on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept();
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();

            Message requestMessage = new Worker().receiveMessage(inputStream);
            if (requestMessage == null) {
                clientSocket.close();
                continue;
            }

            Message responseMessage = new Message();
            responseMessage.magic = "CSM218";
            responseMessage.version = 1;
            responseMessage.sender = "worker";
            responseMessage.timestamp = System.currentTimeMillis();
            responseMessage.messageType = "RESPONSE";

            if ("ROW_MULTIPLY".equals(requestMessage.messageType)) {
                InputStream payloadInputStream = new java.io.ByteArrayInputStream(requestMessage.payload);
                int[][] rowMatrix = Message.unpackMatrix(payloadInputStream);
                int[][] matrixB = Message.unpackMatrix(payloadInputStream);
                int[] resultRow = multiplyRowByMatrix(rowMatrix[0], matrixB);
                java.io.ByteArrayOutputStream payloadOutputStream = new java.io.ByteArrayOutputStream();
                Message.packMatrix(new int[][] { resultRow }, payloadOutputStream);
                responseMessage.payload = payloadOutputStream.toByteArray();
                responseMessage.messageType = "ROW_RESULT";
            } else if ("REGISTER".equals(requestMessage.messageType)) {
                responseMessage.messageType = "REGISTER_ACK";
                responseMessage.payload = "OK".getBytes("UTF-8");
            } else {
                responseMessage.messageType = "ECHO";
                responseMessage.payload = requestMessage.payload;
            }

            new Worker().sendMessage(responseMessage, outputStream);
            clientSocket.close();
        }
    }

    public static int[] multiplyRowByMatrix(int[] row, int[][] B) {
        int cols = B[0].length;
        int[] result = new int[cols];
        for (int j = 0; j < cols; j++) {
            int sum = 0;
            for (int k = 0; k < row.length; k++) {
                sum += row[k] * B[k][j];
            }
            result[j] = sum;
        }
        return result;
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        // Registration handshake with Master
        String workerId = "worker-" + java.util.UUID.randomUUID();
        String studentId = System.getenv("STUDENT_ID");
        if (studentId == null)
            studentId = "unknown";
        try (Socket socket = new Socket(masterHost, port)) {
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();

            Message registrationMessage = new Message();
            registrationMessage.magic = "CSM218";
            registrationMessage.version = 1;
            registrationMessage.messageType = "REGISTER";
            registrationMessage.sender = workerId;
            registrationMessage.timestamp = System.currentTimeMillis();
            registrationMessage.studentId = studentId;
            registrationMessage.payload = "CAPABILITIES:ROW_MULTIPLY".getBytes("UTF-8");

            sendMessage(registrationMessage, outputStream);
            Message acknowledgement = receiveMessage(inputStream);
            if (acknowledgement != null && "REGISTER_ACK".equals(acknowledgement.messageType)) {
                System.out.println("Registered with master: " + new String(acknowledgement.payload, "UTF-8"));
                // Start heartbeat after successful registration
                startHeartbeat(masterHost, port, workerId);
            }
        } catch (IOException exception) {
            System.err.println("Failed to join cluster: " + exception.getMessage());
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // TODO: Implement internal task scheduling
    }
}
