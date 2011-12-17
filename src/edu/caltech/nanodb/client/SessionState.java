package edu.caltech.nanodb.client;


import java.util.concurrent.atomic.AtomicInteger;

import edu.caltech.nanodb.transactions.TransactionState;


/**
 * This class holds all session state associated with a particular client
 * accessing the database.  This object can be stored in thread-local storage
 * to ensure that it can be accessed throughout the database engine to determine
 * current client settings.
 */
public class SessionState {

    /*========================================================================
     * STATIC FIELDS AND METHODS
     */


    /**
     * This static variable holds the next session ID to assign to a client
     * session.  It must be accessed in a synchronized manner.
     */
    static private AtomicInteger nextSessionID = new AtomicInteger(1);

    
    private static ThreadLocal<SessionState> threadLocalState =
        new ThreadLocal<SessionState>() {
            @Override protected SessionState initialValue() {
                return new SessionState(nextSessionID.getAndIncrement());
            }
        };


    /**
     * Returns the current session state, possibly initializing a new session
     * with its own unique ID in the process.  This value is stored in
     * thread-local storage, so no thread-safety is required when manipulating
     * the returned object.
     *
     * @return the session-state for this local thread.
     */
    public static SessionState get() {
        return threadLocalState.get();
    }


    /**
     * Removes the session-state from the thread's thread-local storage.
     */
    public static void remove() {
        threadLocalState.remove();
    }


    /*========================================================================
     * NON-STATIC FIELDS AND METHODS
     */


    /** The unique session ID assigned to this client session. */
    private int sessionID;


    /** The transaction state of this session. */
    private TransactionState txnState;


    private SessionState(int sessionID) {
        this.sessionID = sessionID;
    }


    /**
     * Returns the unique session ID for this client.
     * 
     * @return the unique session ID for this client.
     */
    public int getSessionID() {
        return sessionID;
    }


    public TransactionState getTxnState() {
        return txnState;
    }


    @Override
    public int hashCode() {
        return sessionID;
    }
}
