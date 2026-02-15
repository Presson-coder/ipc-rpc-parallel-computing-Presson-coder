package pdc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The Master acts as the Coordinator in a distributed cluster.
 * 
 * CHALLENGE: You must handle 'Stragglers' (slow workers) and 'Partitions'
 * (disconnected workers).
 * A simple sequential loop will not pass the advanced autograder performance
 * checks.
 */
public class Master {

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

    public static void main(String[] args) throws IOException {
        String host = "localhost";
        int port = 9000;
        Socket worker = new Socket(host, port);

        OutputStream out = worker.getOutputStream();
        InputStream in = worker.getInputStream();

        Message.packString("hello worker 1", out);

        String reply = Message.unpackString(in);
        System.out.println("Received message from worker: " + reply);

        worker.close();
    }


    public Object coordinate(String operation, int[][] data, int workerCount) {
        // TODO: Architect a scheduling algorithm that survives worker failure.
        // HINT: Think about how MapReduce or Spark handles 'Task Reassignment'.
        return null;
    }

    /**
     * Start the communication listener.
     * Use your custom protocol designed in Message.java.
     */
    public void listen(int port) throws IOException {
        // TODO: Implement the listening logic using the custom 'Message.pack/unpack'
        // methods.
    }

    /**
     * System Health Check.
     * Detects dead workers and re-integrates recovered workers.
     */
    public void reconcileState() {
        // TODO: Implement cluster state reconciliation.
    }
}
