package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;


/**
 */
public class TransactionState {

    public static final int NO_TRANSACTION = -1;


    private int transactionID = NO_TRANSACTION;


    private LogSequenceNumber lastLSN = null;


    public int getTransactionID() {
        return transactionID;
    }


    public void setTransactionID(int transactionID) {
        this.transactionID = transactionID;
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
    }
}
