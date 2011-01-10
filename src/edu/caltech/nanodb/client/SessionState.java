package edu.caltech.nanodb.client;


/**
 * This class holds all session state associated with a particular client
 * accessing the database.  This object can be stored in thread-local storage
 * to ensure that it can be accessed throughout the database engine to determine
 * current client settings.
 */
public class SessionState {

    /**
     * This static variable holds the next session ID to assign to a client
     * session.  It must be accessed in a synchronized manner.
     */
    static private int nextSessionID = 1;


    /** The unique session ID assigned to this client session. */
    private int sessionID;


    public SessionState() {
        // Retrieve the current "next session ID" and post-increment it.
        synchronized (SessionState.class) {
            sessionID = nextSessionID++;
        }
    }


    /**
     * Returns the unique session ID for this client.
     * 
     * @return the unique session ID for this client.
     */
    public int getSessionID() {
        return sessionID;
    }


    @Override
    public int hashCode() {
        return sessionID;
    }
}
