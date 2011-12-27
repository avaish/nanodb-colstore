package edu.caltech.nanodb.client;

import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/15/11
 * Time: 12:16 PM
 * To change this template use File | Settings | File Templates.
 */
public class SharedServer {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SharedServer.class);


    public static final int DEFAULT_SERVER_PORT = 12200;


    private int serverPort = DEFAULT_SERVER_PORT;


    private HashMap<Integer, Thread> clientThreads =
        new HashMap<Integer, Thread>();


    public void startup() throws IOException {
        logger.info("Starting shared database server.");

        // Start up the database by doing the appropriate startup processing.
        logger.info("Initializing storage manager.");
        StorageManager.init();

        // Register a shutdown hook so we can shut down the database cleanly.
        Runtime rt = Runtime.getRuntime();
        rt.addShutdownHook(new Thread(new Runnable() {
            public void run() { shutdown(); }
        } ));

        // Start up the server-socket that we receive incoming connections on.
        ServerSocket serverSocket = new ServerSocket(serverPort);

        // Wait for a client to connect.  When one does, spin off a thread to
        // handle requests from that client.
        int clientID = 1;
        while (true) {
            logger.info("Waiting for client connection.");
            Socket sock = serverSocket.accept();
            logger.info("Received client connection.");
            ClientHandler clientHandler = new ClientHandler(clientID, sock);
            Thread t = new Thread(clientHandler);

            // Record the thread so that when the server is being shut down,
            // we can stop all the client threads.
            synchronized (clientThreads) {
                clientThreads.put(clientID, t);
            }

            t.start();
        }
    }


    public void shutdown() {
        for (Thread t : clientThreads.values()) {
            // TODO:  Shut down the client thread.
        }
        
        try {
            StorageManager.shutdown();
        }
        catch (IOException e) {
            logger.error("Couldn't cleanly shut down the Storage Manager!", e);
        }
    }
    
    
    public static void main(String[] args) {
        SharedServer server = new SharedServer();
        try {
            server.startup();
        }
        catch (IOException e) {
            System.out.println("Couldn't start shared server:  " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }
}
