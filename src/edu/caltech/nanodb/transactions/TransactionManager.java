package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.client.SessionState;

import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TransactionManager {

    /** This is the singleton instance of the transaction manager. */
    private static TransactionManager txnManager = null;

    
    public static void init() {
        // TODO:  Set the "next transaction ID" value properly.
        txnManager = new TransactionManager(1);
    }


    /**
     * Returns the singleton instance of the transaction manager.
     *
     * @return the singleton instance of the transaction manager
     */
    public static TransactionManager getInstance() {
        if (txnManager == null) {
            throw new IllegalStateException(
                "TransactionManager has not been initialized");
        }

        return txnManager;
    }


    /**
     * This variable keeps track of the next transaction ID that should be used
     * for a transaction.  It is initialized when the transaction manager is
     * started.
     */
    private AtomicInteger nextTxnID;


    public TransactionManager(int nextTxnID) {
        this.nextTxnID = new AtomicInteger(nextTxnID);
    }


    /**
     * Returns the next transaction ID without incrementing it.  This method is
     * intended to be used when shutting down the database, in order to remember
     * what transaction ID to start with the next time.
     *
     * @return the next transaction ID to use
     */
    public int getNextTxnID() {
        return nextTxnID.get();
    }


    /**
     * Returns the next transaction ID without incrementing it.  This method is
     * intended to be used when shutting down the database, in order to remember
     * what transaction ID to start with the next time.
     *
     * @return the next transaction ID to use
     */
    public int getAndIncrementNextTxnID() {
        return nextTxnID.getAndIncrement();
    }


    public void startTransaction(boolean userStarted) throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (txnState.isTxnInProgress())
            throw new IllegalStateException("A transaction is already in progress!");

        txnState.setTransactionID(getAndIncrementNextTxnID());
        txnState.setUserStartedTxn(userStarted);

        // Don't record a "start transaction" WAL record until the transaction
        // actually writes to something in the database.
    }


    public void commitTransaction() throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (!txnState.isTxnInProgress()) {
            // The user issued a COMMIT without starting a transaction!

            state.getOutputStream().println(
                "No transaction is currently in progress.");

            return;
        }

        // TODO:  COMMIT!

        // Now that the transaction is successfully committed, clear the current
        // transaction state.
        txnState.clear();
    }


    public void rollbackTransaction() throws TransactionException {
        SessionState state = SessionState.get();
        TransactionState txnState = state.getTxnState();

        if (!txnState.isTxnInProgress()) {
            // The user issued a ROLLBACK without starting a transaction!

            state.getOutputStream().println(
                "No transaction is currently in progress.");

            return;
        }

        // TODO:  ROLLBACK!

        // Now that the transaction is successfully committed, clear the current
        // transaction state.
        txnState.clear();
    }
}
