package pdc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {
    private final ConcurrentLinkedQueue<Runnable> failedTasks = new ConcurrentLinkedQueue<>();

    // RPC utility: send a Message and receive a Message over a socket
    public static void sendMessage(Message message, OutputStream outputStream) throws IOException {
        byte[] messageBytes = message.pack();
        outputStream.write(messageBytes);
        outputStream.flush();
    }

    public static Message receiveMessage(InputStream inputStream) throws IOException {
        byte[] lengthBytes = new byte[4];
        int read = inputStream.read(lengthBytes);
        if (read != 4)
            return null;
        int messageLength = ByteBuffer.wrap(lengthBytes).getInt();
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
                ByteBuffer.allocate(4 + messageLength).putInt(messageLength).put(messageBytes).array());
    }

    // Thread-safe queue for pending tasks
    private final ConcurrentLinkedQueue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
    // Atomic integer for task IDs
    private final AtomicInteger taskIdGen = new AtomicInteger(
            0);
    // Track last heartbeat time for each worker
    private final ConcurrentHashMap<String, Long> workerHeartbeats = new ConcurrentHashMap<>();

    // Thread-safe map to track registered workers and their capabilities
    private final ConcurrentHashMap<String, String> workerCapabilities = new ConcurrentHashMap<>();

    // Start a background thread to check for failed workers
    {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            for (String workerId : workerHeartbeats.keySet()) {
                long last = workerHeartbeats.get(workerId);
                if (now - last > 5000) { // 5 seconds timeout
                    System.out
                            .println("[FAILURE DETECTED] Worker " + workerId + " missed heartbeat. Marking as failed.");
                    workerHeartbeats.remove(workerId);
                    workerCapabilities.remove(workerId);
                }
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    // Overloaded method for test compatibility
    /**
     * Listen for incoming connections.
     * 
     * @param port the port to listen on
     */
    public void listen(int port) {
        // TODO: Implement listen logic
    }

    public Object coordinate(String task, int[][] matrix, int workers) {
        // Initial stub for test compatibility
        return null;
    }

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();

    /**
     * Entry point for a distributed computation.
     * 
     * Students must:
     * 1. Partition the problem into independent 'computational units'.
     * 2. Schedule units across a dynamic pool of workers.
     * 3. Handle result aggregation while maintaining thread safety.
     * 
     * @param operation A string descriptor of the matrix operation (e.g.
     *                  "BLOCK_MULTIPLY")
     * @param data      The raw matrix data to be processed
     */

    public static void main(String[] args) {
        Master master = new Master();
        int[][] A = {
                { 1, 2 },
                { 3, 4 }
        };
        int[][] B = {
                { 5, 6 },
                { 7, 8 }
        };
        Object result = master.coordinate("ROW_MULTIPLY", A, B, 1);

        if (result instanceof int[][]) {
            int[][] matrix = (int[][]) result;
            System.out.println("Result matrix:");
            for (int[] row : matrix) {
                for (int val : row) {
                    System.out.print(val + " ");
                }
                System.out.println();
            }
        } else {
            System.out.println("No matrix received.");
        }
    }

    public Object coordinate(String operation, int[][] A, int[][] B, int workerCount) {
        // Monitor for failed tasks and resubmit them
        Thread recoveryThread = new Thread(() -> {
            while (true) {
                Runnable failedTask = failedTasks.poll();
                if (failedTask != null) {
                    systemThreads.submit(failedTask);
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
        });
        recoveryThread.setDaemon(true);
        recoveryThread.start();
        int rowCount = A.length;
        int columnCount = B[0].length;
        int[][] resultMatrix = new int[rowCount][columnCount];

        java.util.concurrent.ConcurrentHashMap<Integer, int[]> rowResults = new java.util.concurrent.ConcurrentHashMap<>();
        List<Future<?>> futureList = new ArrayList<>();

        for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
            final int currentRowIndex = rowIndex;
            final int taskId = taskIdGen.incrementAndGet();
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    try {
                        String studentId = System.getenv("STUDENT_ID");
                        if (studentId == null)
                            studentId = "unknown";
                        String host = "localhost";
                        int port = 9000;
                        Socket workerSocket = new Socket(host, port);
                        OutputStream outputStream = workerSocket.getOutputStream();
                        InputStream inputStream = workerSocket.getInputStream();

                        // Prepare payload: [rowMatrix][B]
                        java.io.ByteArrayOutputStream payloadOutputStream = new java.io.ByteArrayOutputStream();
                        Message.packMatrix(new int[][] { A[currentRowIndex] }, payloadOutputStream);
                        Message.packMatrix(B, payloadOutputStream);
                        byte[] payload = payloadOutputStream.toByteArray();

                        Message requestMessage = new Message();
                        requestMessage.magic = "CSM218";
                        requestMessage.version = 1;
                        requestMessage.messageType = "ROW_MULTIPLY";
                        requestMessage.sender = "master";
                        requestMessage.studentId = studentId;
                        requestMessage.timestamp = System.currentTimeMillis();
                        requestMessage.payload = payload;

                        sendMessage(requestMessage, outputStream);
                        Message responseMessage = receiveMessage(inputStream);
                        if (responseMessage != null && responseMessage.payload != null
                                && "ROW_RESULT".equals(responseMessage.messageType)) {
                            InputStream payloadInputStream = new java.io.ByteArrayInputStream(responseMessage.payload);
                            int[][] rowResult = Message.unpackMatrix(payloadInputStream);
                            rowResults.put(currentRowIndex, rowResult[0]);
                        } else {
                            // If no response or wrong type, consider as failed and retry
                            failedTasks.add(this);
                        }
                        workerSocket.close();
                    } catch (IOException exception) {
                        // On exception, consider as failed and retry
                        failedTasks.add(this);
                    }
                }
            };
            taskQueue.add(task);
            futureList.add(systemThreads.submit(task));
        }

        // Wait for all tasks to complete
        for (Future<?> future : futureList) {
            try {
                future.get();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
        // Aggregate results
        for (int i = 0; i < rowCount; i++) {
            int[] row = rowResults.get(i);
            if (row != null) {
                resultMatrix[i] = row;
            }
        }
        return resultMatrix;
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        // TODO: Implement cluster state reconciliation.
    }
}
