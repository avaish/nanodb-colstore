package edu.caltech.nanodb.storage.writeahead;


import edu.caltech.nanodb.storage.BufferManager;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.HashMap;

import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBFileReader;
import edu.caltech.nanodb.storage.DBFileWriter;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionState;
import edu.caltech.nanodb.util.ArrayUtil;
import edu.caltech.nanodb.util.NoCopyByteArrayOutputStream;


/**
 * This class manages the write-ahead log of the database.  The format of the
 * write-ahead log file is as follows:
 *
 * <ul>
 *   <li>The write-ahead log file is read and written in units of blocks, as is
 *       the norm for database files.</li>
 *   <li>Write-ahead logs need to be able to be scanned in both forward and
 *       reverse directions.</li>
 * </ul>
 */
public class WALManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(WALManager.class);


    /** Write-ahead log files follow this pattern. */
    public static final String WAL_FILENAME_PATTERN = "wal-%05d.log";


    /** This is a regex pattern for write-ahead log filenames. */
    public static final String WAL_FILENAME_REGEX = "wal-(\\d)\\.log";


    /**
     * Maximum file number for a write-ahead log file.
     */
    public static final int MAX_WAL_FILE_NUMBER = 65535;


    /**
     * Maximum size of a write-ahead log file is 10MB.  When the current WAL
     * file reaches this size, it is closed and a new WAL file is created with
     * the next increasing file number.
     */
    public static final int MAX_WAL_FILE_SIZE = 10 * 1024 * 1024;


    /**
     * This is the file-offset of the first log entry in a WAL file.  It must
     * be at least 2, so that the file's type and page-size can be stored in
     * the first two bytes.
     */
    public static final int WAL_FILE_INITIAL_OFFSET = 2;

    
    public static class RecoveryInfo {
        /** This is the log sequence number to start recovery processing from. */
        public LogSequenceNumber firstLSN;


        /**
         * This is the "next LSN", one past the last valid log sequence number
         * found in the write-ahead logs.
         */
        public LogSequenceNumber nextLSN;


        /**
         * This is the maximum transaction ID seen in the write-ahead logs.
         * The next transaction ID used by the database system will be one more
         * than this value.
         */
        public int maxTransactionID;
        
        /**
         * This is the set of incomplete transactions found during recovery
         * processing, along with the last log sequence number seen for each
         * transaction.
         */
        public HashMap<Integer, LogSequenceNumber> incompleteTxns;


        public RecoveryInfo(LogSequenceNumber firstLSN,
                            LogSequenceNumber nextLSN) {

            this.firstLSN = firstLSN;
            this.nextLSN = nextLSN;
            
            this.maxTransactionID = -1;
            
            incompleteTxns = new HashMap<Integer, LogSequenceNumber>();
        }


        /**
         * This helper method updates the recovery information with the
         * specified transaction ID and log sequence number.  The requirement is
         * that this method is only used during redo processing; we expect that
         * log sequence numbers are monotonically increasing.
         *
         * @param transactionID the ID of the transaction that appears in the
         *        current write-ahead log record
         *
         * @param lsn the log sequence number of the current write-ahead log
         *        record
         */
        public void updateInfo(int transactionID, LogSequenceNumber lsn) {
            incompleteTxns.put(transactionID, lsn);

            if (transactionID > maxTransactionID)
                maxTransactionID = transactionID;
        }


        /**
         * This helper function records that the specified transaction is
         * completed in the write-ahead log.  Specifically, the transaction is
         * removed from the set of incomplete transactions.
         *
         * @param transactionID the transaction to record as completed
         */
        public void recordTxnCompleted(int transactionID) {
            incompleteTxns.remove(transactionID);
        }


        /**
         * Returns true if there are any incomplete transactions, or false if
         * all transactions are completed.
         *
         * @return true if there are any incomplete transactions, or false if
         *         all transactions are completed.
         */
        public boolean hasIncompleteTxns() {
            return !incompleteTxns.isEmpty();
        }


        /**
         * Returns true if the specified transaction is complete, or false if
         * it appears in the set of incomplete transactions.
         *
         * @param transactionID the transaction to check for completion status
         *
         * @return true if the transaction is complete, or false otherwise
         */
        public boolean isTxnComplete(int transactionID) {
            return !incompleteTxns.containsKey(transactionID);
        }
    }


    /**
     * This object holds the log sequence number where the next write-ahead log
     * record will be written.
     */
    LogSequenceNumber nextLSN;


    private StorageManager storageManager;
    
    
    private BufferManager bufferManager;


    public WALManager(StorageManager storageManager,
                      BufferManager bufferManager) {
        this.storageManager = storageManager;
        this.bufferManager = bufferManager;
    }


    /**
     * Performs recovery processing starting at the specified log sequence
     * number, and returns the LSN where the next recovery process should start
     * from.
     *
     * @param firstLSN the location in the write-ahead log where recovery should
     *        start from
     *
     * @return the new location where recovery should start from the next time
     *         recovery processing is performed
     */
    public LogSequenceNumber doRecovery(LogSequenceNumber firstLSN,
        LogSequenceNumber nextLSN) throws IOException {

        RecoveryInfo recoveryInfo = new RecoveryInfo(firstLSN, nextLSN);

        performRedo(recoveryInfo);

        this.nextLSN = nextLSN;
        performUndo(recoveryInfo);

        return this.nextLSN;
    }


    private void performRedo(RecoveryInfo recoveryInfo) throws IOException {
        LogSequenceNumber currLSN = recoveryInfo.firstLSN;
        logger.debug("Starting redo processing at LSN " + currLSN);

        LogSequenceNumber oldLSN = null;
        DBFileReader walReader = null;
        while (currLSN.compareTo(recoveryInfo.nextLSN) < 0) {
            if (oldLSN == null || oldLSN.getLogFileNo() != currLSN.getLogFileNo())
                walReader = getWALFileReader(currLSN);

            // Read the parts of the log record that are always the same.
            byte typeID = walReader.readByte();
            int transactionID = walReader.readInt();
            WALRecordType type = WALRecordType.valueOf(typeID);

            if (type != WALRecordType.START_TXN) {
                // The "start transaction record is different because it doesn't
                // have a "previous LSN" value, but everything else does.
    
                // During redo processing we don't use the previous LSN, so just
                // skip over it.  File # is a short, and the offset is an int.
                walReader.movePosition(6);
            }

            // Update our general recovery info, namely the LSN of the last
            // record we have seen for each transaction, and also the maximum
            // transaction ID we have seen.
            recoveryInfo.updateInfo(transactionID, currLSN);

            // Redo specific operations.
            switch (type) {
            case START_TXN:
                // Already handled by RecoveryInfo's updateInfo() operation.
                break;

            case COMMIT_TXN:
            case ABORT_TXN:
                recoveryInfo.recordTxnCompleted(transactionID);
                break;

            case UPDATE_PAGE:
            case UPDATE_PAGE_REDO_ONLY:
                // TODO:  Reapply the changes to the specified file and page.



                break;

            default:
                // TODO:  This should probably be a special kind of exception.
                throw new IllegalStateException(
                    "Encountered unrecognized WAL record type " + type +
                    " at LSN " + currLSN + " during redo processing!");
            }

            oldLSN = currLSN;
            currLSN = computeNextLSN(currLSN.getLogFileNo(), walReader.getPosition());
        }
        
        if (currLSN.compareTo(recoveryInfo.nextLSN) != 0) {
            // TODO:  This should probably be a special kind of exception.
            throw new IllegalStateException("Traversing WAL file didn't yield " +
                " the same ending LSN as in the transaction-state file.  WAL " +
                " result:  " + currLSN + "  TxnState:  " + recoveryInfo.nextLSN);
        }

        logger.debug("Redo processing is complete.  There are " +
            recoveryInfo.incompleteTxns.size() + " incomplete transactions.");
    }


    private void performUndo(RecoveryInfo recoveryInfo) {
        // TODO:  Perform undo processing on incomplete transactions.
    }


    private LogSequenceNumber computeNextLSN(int fileNo, int fileOffset) {
        if (fileOffset >= MAX_WAL_FILE_SIZE) {
            // This WAL file has reached the size limit.  Increment the file
            // number, wrapping around if necessary, and reset the offset to 0.
            fileNo += 1;
            if (fileNo > MAX_WAL_FILE_NUMBER)
                fileNo = 0;

            // Need to make sure we skip past the file type and size at the
            // start of the data file.
            fileOffset = WAL_FILE_INITIAL_OFFSET;
        }
        return new LogSequenceNumber(fileNo, fileOffset);
    }


    /**
     * Opens the WAL file specified by the given log sequence number, and seeks
     * to the offset specified in the log sequence number.
     * 
     * @param lsn The log sequence number specifying the WAL file and the offset
     *            in the WAL file to go to.
     *
     * @return the WAL file, with the file position moved to the specified
     *         offset.
     *
     * @throws IOException if the corresponding WAL file cannot be opened, or
     *         if some other IO error occurs.
     */
    private DBFileWriter getWALFileWriter(LogSequenceNumber lsn)
        throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile;
        try {
            walFile = storageManager.openWALFile(fileNo);
        }
        catch (FileNotFoundException e) {
            walFile = storageManager.createWALFile(fileNo);
        }

        DBFileWriter writer = new DBFileWriter(walFile);
        writer.setPosition(offset);

        return writer;
    }


    private DBFileReader getWALFileReader(LogSequenceNumber lsn)
        throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile = storageManager.openWALFile(fileNo);
        DBFileReader reader = new DBFileReader(walFile);
        reader.setPosition(offset);

        return reader;
    }



    /**
     * This function writes a transaction demarcation record
     * ({@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN}, or
     * {@link WALRecordType#ABORT_TXN}) to the write-ahead log.  The transaction
     * state is retrieved from thread-local storage so that it doesn't need to
     * be passed.
     *
     * @param type The type of the transaction demarcation to write, one of the
     *        values {@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN},
     *        or {@link WALRecordType#ABORT_TXN}.
     *
     * @throws IOException if the write-ahead log can't be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>type</tt> is <tt>null</tt>, or if
     *         it isn't one of the values {@link WALRecordType#START_TXN},
     *         {@link WALRecordType#COMMIT_TXN}, or {@link WALRecordType#ABORT_TXN}.
     */
    public synchronized void writeTxnRecord(WALRecordType type)
        throws IOException {

        if (type != WALRecordType.START_TXN &&
            type != WALRecordType.COMMIT_TXN &&
            type != WALRecordType.ABORT_TXN) {
            throw new IllegalArgumentException("Invalid record type " + type +
                " passed to writeTxnRecord().");
        }

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        logger.debug("Writing a " + type + " record for transaction " +
            txnState.getTransactionID() + " at LSN " + nextLSN);

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(nextLSN);

        walWriter.writeByte(type.getID());
        walWriter.writeInt(txnState.getTransactionID());

        if (type == WALRecordType.START_TXN) {
            walWriter.writeByte(type.getID());
        }
        else {
            LogSequenceNumber prevLSN = txnState.getLastLSN();
            walWriter.writeShort(prevLSN.getLogFileNo());
            walWriter.writeInt(prevLSN.getFileOffset());
            walWriter.writeByte(type.getID());
        }

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
        logger.debug("Next-LSN value is now " + nextLSN);

        // In the case of committing, we need to sync the write-ahead log file
        // to the disk so that we can actually report the transaction as
        // "committed".
        if (type == WALRecordType.COMMIT_TXN) {
            // Sync the write-ahead log to disk.
            // TODO storageManager.syncWALFile(walFile);

            // TODO:  This should probably be done by the transaction manager.
            txnState.clear();
        }
        else {
            txnState.setLastLSN(nextLSN);
        }
    }


    /**
     * This method writes an update-page record to the write-ahead log,
     * including both undo and redo details.
     *
     * @param dbPage The data page whose changes are to be recorded in the log.
     *
     * @throws IOException if the write-ahead log cannot be updated for some
     *         reason.
     * 
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if it shows no updates.
     */
    public synchronized void writeUpdatePageRecord(DBPage dbPage)
        throws IOException {

        if (dbPage == null)
            throw new IllegalArgumentException("dbPage must be specified");

        if (!dbPage.isDirty())
            throw new IllegalArgumentException("dbPage has no updates to store");

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        logger.debug("Writing an " + WALRecordType.UPDATE_PAGE +
            " record for transaction " + txnState.getTransactionID() +
            " at LSN " + nextLSN);

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(nextLSN);

        walWriter.writeByte(WALRecordType.UPDATE_PAGE.getID());
        walWriter.writeInt(txnState.getTransactionID());

        // We need to store the previous log sequence number for this record.
        LogSequenceNumber prevLSN = txnState.getLastLSN();
        walWriter.writeShort(prevLSN.getLogFileNo());
        walWriter.writeInt(prevLSN.getFileOffset());

        walWriter.writeVarString255(dbPage.getDBFile().getDataFile().getName());
        walWriter.writeShort(dbPage.getPageNo());

        // This offset is where we will store the number of data segments we
        // need to record.
        int segCountOffset = walWriter.getPosition();
        walWriter.writeShort(-1);
        
        byte[] oldData = dbPage.getOldPageData();
        byte[] newData = dbPage.getPageData();
        int pageSize = dbPage.getPageSize();
        int numSegments = 0;
        int index = 0;
        while (index < pageSize) {
            logger.debug("Skipping identical bytes starting at index " + index);
            
            // Skip data until we find stuff that's different.
            index += ArrayUtil.sizeOfIdenticalRange(oldData, newData, index);
            assert index <= pageSize;
            if (index == pageSize)
                break;

            logger.debug("Recording changed bytes starting at index " + index);

            // Find out how much data is actually changed.  We lump in small
            // runs of unchanged data just to make things more efficient.
            int size = 0;
            while (index + size < pageSize) {
                size += ArrayUtil.sizeOfDifferentRange(oldData, newData,
                    index + size);
                assert index + size <= pageSize;
                if (index + size == pageSize)
                    break;

                // If there are 4 or less identical bytes after the different
                // bytes, include them in this segment.
                int sameSize = ArrayUtil.sizeOfIdenticalRange(oldData, newData,
                    index + size);

                if (sameSize > 4 || index + size + sameSize == pageSize)
                    break;

                size += sameSize;
            }

            logger.debug("Found " + size + " changed bytes starting at index " +
                index);

            // Write the starting index within the page, and the amount of
            // data that will be recorded at that index.
            walWriter.writeShort(index);
            walWriter.writeShort(size);

            // Write the old data (undo), and then the new data (redo).
            walWriter.write(oldData, index, size);
            walWriter.write(newData, index, size);

            numSegments++;

            index += size;
        }
        assert index == pageSize;

        // Now that we know how many segments were recorded, store that value
        // at the appropriate location.
        int currOffset = walWriter.getPosition();
        walWriter.setPosition(segCountOffset);
        walWriter.writeShort(numSegments);
        walWriter.setPosition(currOffset);

        // Write the start of the update record at the end so that we can get
        // back to the record's start when scanning the log backwards.

        walWriter.writeInt(nextLSN.getFileOffset());
        walWriter.writeByte(WALRecordType.UPDATE_PAGE.getID());

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
        txnState.setLastLSN(nextLSN);
    }


    /**
     * This method writes an update-page record to the write-ahead log,
     * including both undo and redo details.
     *
     * @param dbPage The data page whose changes are to be recorded in the log.
     * @param numSegments The number of segments in the change-data to record.
     * @param changes The actual changes themselves, serialized to a byte array.
     *
     * @throws IOException if the write-ahead log cannot be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if <tt>changes</tt> is <tt>null</tt>.
     */
    public synchronized void writeRedoOnlyUpdatePageRecord(DBPage dbPage,
        int numSegments, byte[] changes) throws IOException {

        if (dbPage == null)
            throw new IllegalArgumentException("dbPage must be specified");

        if (changes == null)
            throw new IllegalArgumentException("changes must be specified");

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(nextLSN);

        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());
        walWriter.writeInt(txnState.getTransactionID());

        // We need to store the previous log sequence number for this record.
        LogSequenceNumber prevLSN = txnState.getLastLSN();
        walWriter.writeShort(prevLSN.getLogFileNo());
        walWriter.writeInt(prevLSN.getFileOffset());

        walWriter.writeVarString255(dbPage.getDBFile().getDataFile().getName());
        walWriter.writeShort(dbPage.getPageNo());

        // Write the redo-only data.
        walWriter.writeShort(numSegments);
        walWriter.write(changes);

        // Write the start of the update record at the end so that we can get
        // back to the record's start when scanning the log backwards.

        walWriter.writeInt(nextLSN.getFileOffset());
        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
        txnState.setLastLSN(nextLSN);
    }


    public synchronized void rollbackTransaction() throws IOException {
        // Get the details for the transaction to rollback.
        TransactionState txnState = SessionState.get().getTxnState();

        int transactionID = txnState.getTransactionID();
        if (transactionID == TransactionState.NO_TRANSACTION) {
            logger.info("No transaction in progress - rollback is a no-op.");
            return;
        }

        LogSequenceNumber lsn = txnState.getLastLSN();

        logger.info("Rolling back transaction " + transactionID +
            ".  Last LSN = " + lsn);

        // Scan backward through the log records for this transaction to roll
        // it back.
        
        while (true) {
            DBFileReader walReader = getWALFileReader(lsn);

            WALRecordType type = WALRecordType.valueOf(walReader.readByte());
            int recordTxnID = walReader.readInt();
            if (recordTxnID != transactionID) {
                throw new RuntimeException(
                    "FATAL ERROR:  Write-ahead log is corrupt!");
            }

            if (type == WALRecordType.START_TXN) {
                // Done rolling back the transaction.
                break;
            }

            // Read out the "previous LSN" value.
            int prevFileNo = walReader.readUnsignedShort();
            int prevOffset = walReader.readInt();
            LogSequenceNumber prevLSN =
                new LogSequenceNumber(prevFileNo, prevOffset);

            if (type == WALRecordType.UPDATE_PAGE) {
                // Undo this change.

                // Read the file and page with the changes to undo.
                String filename = walReader.readVarString255();
                int pageNo = walReader.readUnsignedShort();

                int numSegments = walReader.readUnsignedShort();

                // Open the specified file and retrieve the data page,
                // then apply the undo data to the page.

                NoCopyByteArrayOutputStream redoOnlyBAOS =
                    new NoCopyByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(redoOnlyBAOS);

                DBFile dbFile = storageManager.openDBFile(filename);
                DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
                byte[] data = dbPage.getPageData();

                for (int i = 0; i < numSegments; i++) {
                    // Apply the undo data to the data page.
                    int start = walReader.readUnsignedShort();
                    int length = walReader.readUnsignedShort();
                    walReader.read(data, start, length);

                    // Record what we wrote for the redo-only record.
                    dos.writeShort(start);
                    dos.writeShort(length);
                    dos.write(data, start, length);
                }

                // Write a redo-only update record to the end of the WAL.
                dos.flush();
                writeRedoOnlyUpdatePageRecord(dbPage, numSegments,
                    redoOnlyBAOS.getBuf());
            }
            else {
                logger.warn(String.format("Encountered unexpected WAL-record " +
                    "type %s while rolling back transaction %d.", type,
                    transactionID));
            }

            // Go to the immediately preceding record in the logs for this
            // transaction.
            lsn = prevLSN;
        }

        // All done rolling back the transaction!  Record that it was aborted
        // in the WAL.
        writeTxnRecord(WALRecordType.ABORT_TXN);
        logger.info(String.format("Transaction %d:  Rollback complete.",
            transactionID));
    }
}
