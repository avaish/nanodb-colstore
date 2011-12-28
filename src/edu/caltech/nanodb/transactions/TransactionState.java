package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;


/**
 * <p>
 * This class manages the transaction state associated with every client
 * session.  The transaction state for the current session can be retrieved
 * like this:
 * </p>
 * <pre>    TransactionState txnState = SessionState.get().getTxnState()</pre>
 * <p>
 * <b>The transaction state should generally <u>not</u> be managed directly!</b>
 * Rather, the operations provided by the {@link TransactionManager} should be
 * used.
 * </p>
 */
public class TransactionState {

    public static final int NO_TRANSACTION = -1;


    private int transactionID = NO_TRANSACTION;


    private boolean userStartedTxn = false;


    private LogSequenceNumber lastLSN = null;


    public int getTransactionID() {
        return transactionID;
    }


    public void setTransactionID(int transactionID) {
        this.transactionID = transactionID;
    }


    public boolean getUserStartedTxn() {
        return userStartedTxn;
    }


    public void setUserStartedTxn(boolean b) {
        userStartedTxn = b;
    }


    public LogSequenceNumber getLastLSN() {
        return lastLSN;
    }


    public void setLastLSN(LogSequenceNumber lsn) {
        lastLSN = lsn;
    }


    public void clear() {
        transactionID = NO_TRANSACTION;
        lastLSN = null;
        userStartedTxn = false;
    }


    public boolean isTxnInProgress() {
        return (transactionID != NO_TRANSACTION);
    }
}
