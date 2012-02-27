package edu.caltech.nanodb.transactions;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import edu.caltech.nanodb.storage.BufferManager;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileManager;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.server.EventDispatcher;
import edu.caltech.nanodb.storage.writeahead.WALManager;
import edu.caltech.nanodb.storage.writeahead.WALRecordType;


/**
 */
public class TransactionManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(TransactionManager.class);


    /**
     * The system property that can be used to turn on or off transaction
     * processing.
     */
    public static final String PROP_TXNS = "nanodb.transactions";


    /**
     * This is the name of the file that the Transaction Manager uses to keep
     * track of overall transaction state.
     */
    public static final String TXNSTATE_FILENAME = "txnstate.dat";



    
    public static boolean isEnabled() {
        return "on".equalsIgnoreCase(System.getProperty(PROP_TXNS, "on"));
    }

    
    private StorageManager storageManager;
    
    
    private BufferManager bufferManager;
    
    
    private FileManager fileManager;
    
    
    private WALManager walManager;
    

    /**
     * This variable keeps track of the next transaction ID that should be used
     * for a transaction.  It is initialized when the transaction manager is
     * started.
     */
    private AtomicInteger nextTxnID;


    public TransactionManager(StorageManager storageManager,
        BufferManager bufferManager, FileManager fileManager) {

        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
        this.fileManager = fileManager;

        this.nextTxnID = new AtomicInteger();

        // TODO:  Pass buffer manager to the WAL manager too
        walManager = new WALManager(storageManager, bufferManager);
        storageManager.addFileTypeManager(DBFileType.WRITE_AHEAD_LOG_FILE, walManager);
    }


    /**
     * This helper function initializes a brand new transaction-state file for
     * the transaction manager to use for providing transaction atomicity and
     * durability.
     *
     * @throws IOException if the transaction-state file can't be created for
     *         some reason.
     */
    public DBFile createTxnStateFile() throws IOException {
        // Create a brand new transaction-state file for the Transaction Manager
        // to use.

        DBFile dbfTxnState;
        dbfTxnState = storageManager.createDBFile(TXNSTATE_FILENAME,
            DBFileType.TXNSTATE_FILE);

        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        // Set the "next transaction ID" value to an initial default.
        txnState.setNextTransactionID(1);

        // Set the "first LSN" and "next LSN values to initial defaults.
        LogSequenceNumber lsn =
            new LogSequenceNumber(0, WALManager.WAL_FILE_INITIAL_OFFSET);

        txnState.setFirstLSN(lsn);
        txnState.setNextLSN(lsn);

        fileManager.saveDBPage(dbpTxnState);
        fileManager.syncDBFile(dbfTxnState);

        return dbfTxnState;
    }


    public void initialize() throws IOException {
        if (!isEnabled())
            throw new IllegalStateException("Transactions are disabled!");

        // Read the transaction-state file so we can initialize the
        // Transaction Manager.

        DBFile dbfTxnState;
        try {
            dbfTxnState = storageManager.openDBFile(TXNSTATE_FILENAME);
        }
        catch (FileNotFoundException e) {
            // TODO:  If we find any other files in the data directory, we
            //        really should fail initialization, because the old files
            //        may have been created without transaction processing...

            logger.info("Couldn't find transaction-state file " +
                TXNSTATE_FILENAME + ", creating.");

            dbfTxnState = createTxnStateFile();
        }

        DBPage dbpTxnState = storageManager.loadDBPage(dbfTxnState, 0);
        TransactionStatePage txnState = new TransactionStatePage(dbpTxnState);

        // Set the "next transaction ID" value properly.
        nextTxnID.set(txnState.getNextTransactionID());

        // Retrieve the "first LSN" and "next LSN values so we know the range of
        // the write-ahead log that we need to apply for recovery.
        LogSequenceNumber firstLSN = txnState.getFirstLSN();
        LogSequenceNumber nextLSN = txnState.getNextLSN();

        // Perform recovery, and get the new "first LSN" value
        // TODO:  This probably needs to be a "recovery info" object, because we need the next transaction-ID too.
        LogSequenceNumber newFirstLSN = walManager.doRecovery(firstLSN, nextLSN);

        // TODO:  Set the "next transaction ID" value based on what recovery found

        // TODO:  Update and sync the transaction state if any changes were made.

        // Register the component that manages indexes when tables are modified.
        EventDispatcher.getInstance().addCommandEventListener(
            new TransactionStateUpdater());
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

        int txnID = getAndIncrementNextTxnID();
        txnState.setTransactionID(txnID);
        txnState.setUserStartedTxn(userStarted);
        
        logger.debug("Starting transaction with ID " + txnID +
            (userStarted ? " (user-started)" : ""));

        // Don't record a "start transaction" WAL record until the transaction
        // actually writes to something in the database.
    }


    public void recordPageUpdate(DBPage dbPage) throws IOException {
        if (!dbPage.isDirty()) {
            logger.debug("Page reports it is not dirty; not logging update.");
            return;
        }

        logger.debug("Recording page-update for page " + dbPage.getPageNo() +
            " of file " + dbPage.getDBFile());

        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.hasLoggedTxnStart()) {
            walManager.writeTxnRecord(WALRecordType.START_TXN);
            txnState.setLoggedTxnStart(true);
        }

        walManager.writeUpdatePageRecord(dbPage);
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

        int txnID = txnState.getTransactionID();
        
        if (txnState.hasLoggedTxnStart()) {
            // Must record the transaction as committed to the write-ahead log.
            try {
                walManager.writeTxnRecord(WALRecordType.COMMIT_TXN);
            }
            catch (IOException e) {
                throw new TransactionException("Couldn't commit transaction " +
                    txnID + "!", e);
            }
        }
        else {
            logger.debug("Transaction " + txnID + " has made no changes; not " +
                "recording transaction-commit to WAL.");
        }

        // Now that the transaction is successfully committed, clear the current
        // transaction state.
        logger.debug("Transaction completed, resetting transaction state.");
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

        int txnID = txnState.getTransactionID();

        if (txnState.hasLoggedTxnStart()) {
            // Must rollback the transaction using the write-ahead log.
            try {
                walManager.rollbackTransaction();
            }
            catch (IOException e) {
                throw new TransactionException(
                    "Couldn't rollback transaction " + txnID + "!", e);
            }
        }
        else {
            logger.debug("Transaction " + txnID + " has made no changes; not " +
                "recording transaction-rollback to WAL.");
        }

        // Now that the transaction is successfully rolled back, clear the
        // current transaction state.
        logger.debug("Transaction completed, resetting transaction state.");
        txnState.clear();
    }
}
