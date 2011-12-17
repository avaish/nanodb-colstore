package edu.caltech.nanodb.client;

import org.apache.log4j.Logger;

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


    private static class ClientHandler implements Runnable {
        private int id;
        private Socket sock;

        public ClientHandler(int id, Socket sock) {
            this.id = id;
            this.sock = sock;
        }

        public void run() {
            ObjectInputStream ois = new ObjectInputStream(sock.getInputStream());
            ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());

            while (true) {
                try {
                    ois.readObject();
                }
                catch (EOFException e) {
                    logger.info(String.format("Client %d disconnected.\n", id));
                    break;
                }
                catch (IOException e) {

                }
                catch (ClassNotFoundException e) {

                }
            }
        }
    }



    public void start() throws IOException {
        // TODO:  Start up the database by doing the appropriate startup processing.

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
            Socket sock = serverSocket.accept();
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
        for (Thread t : clientThreads) {

        }
    }
}
