package pdc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    public static void main(String[] args) throws IOException {
        int port = 9000;
        ServerSocket server = new ServerSocket(port);
        System.out.println("Worker started, listening on port " + port);

        Socket client = server.accept();
        InputStream in = client.getInputStream();
        OutputStream out = client.getOutputStream();

        String msg = Message.unpackString(in);
        System.out.println("Received message from master: " + msg);
        
        Message.packString("Hello Master, this is Worker!", out);

        client.close();
        server.close();
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        // TODO: Implement the cluster join protocol
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
