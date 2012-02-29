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
     * This is the file-offset just past the last byte written in the previous
     * WAL file, or 0 for the first WAL file.  The value is an integer,
     * occupying 4 bytes.
     */
    public static final int OFFSET_PREV_FILE_END = 2;


    /**
     * This is the file-offset of the first log entry in a WAL file.
     */
    public static final int OFFSET_FIRST_RECORD = 6;



    
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
        
        
        public LogSequenceNumber getLastLSN(int transactionID) {
            return incompleteTxns.get(transactionID);
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


    private StorageManager storageManager;


    /**
     * This object holds the log sequence number of the first write-ahead log
     * record where recovery would need to start from.
     */
    private LogSequenceNumber firstLSN;


    /**
     * This object holds the log sequence number where the next write-ahead log
     * record will be written.
     */
    private LogSequenceNumber nextLSN;


    public WALManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    public LogSequenceNumber getFirstLSN() {
        return firstLSN;
    }


    public LogSequenceNumber getNextLSN() {
        return nextLSN;
    }


    /**
     * Performs recovery processing starting at the specified log sequence
     * number, and returns the LSN where the next recovery process should start
     * from.
     *
     * @param storedFirstLSN the location of the write-ahead log record where
     *        recovery should start from
     *
     * @param storedNextLSN the location in the write-ahead log that is
     *        <em>just past</em> the last valid log record in the WAL
     *
     * @return the new location where recovery should start from the next time
     *         recovery processing is performed
     *         
     * @throws IOException if an IO error occurs during recovery processing
     */
    public RecoveryInfo doRecovery(LogSequenceNumber storedFirstLSN,
        LogSequenceNumber storedNextLSN) throws IOException {

        RecoveryInfo recoveryInfo = new RecoveryInfo(firstLSN, nextLSN);
        this.firstLSN = storedFirstLSN;
        this.nextLSN = storedNextLSN;

        if (firstLSN.equals(nextLSN)) {
            // No recovery necessary!  Just return the passed-in info.
            return recoveryInfo;
        }

        performRedo(recoveryInfo);
        performUndo(recoveryInfo);

        // Force the WAL out, up to the nextLSN value.  Then, flush and sync all
        // table files.
        storageManager.forceWAL(nextLSN);
        storageManager.closeAllOpenTables();  // TODO:  not just tables!  and sync them too!

        // At this point, all files in the database should be in sync with
        // the entirety of the write-ahead log.  So, update the firstLSN value
        // and update the transaction state file again.  (This won't write out
        // any WAL records, but it will write and sync the txn-state file.)
        firstLSN = nextLSN;
        storageManager.forceWAL(nextLSN);

        recoveryInfo.firstLSN = firstLSN;
        recoveryInfo.nextLSN = nextLSN;

        return recoveryInfo;
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
                logger.debug("Transaction " + transactionID + " is starting");
                break;

            case COMMIT_TXN:
            case ABORT_TXN:
                logger.debug("Transaction " + transactionID +
                    " is completed (" + type + ")");
                recoveryInfo.recordTxnCompleted(transactionID);
                break;

            case UPDATE_PAGE:
            case UPDATE_PAGE_REDO_ONLY:
                // Reapply the changes to the specified file and page.

                String redoFilename = walReader.readVarString255();
                int redoPageNo = walReader.readUnsignedShort();

                DBFile redoFile = storageManager.openDBFile(redoFilename);
                DBPage redoPage = storageManager.loadDBPage(redoFile, redoPageNo);

                int numSegments = walReader.readUnsignedShort();

                logger.debug(String.format(
                    "Redoing changes to file %s, page %d (%d segments)",
                    redoFile, redoPageNo, numSegments));

                for (int iSeg = 0; iSeg < numSegments; iSeg++) {
                    // Write the starting index within the page, and the amount of
                    // data that will be recorded at that index.
                    int index = walReader.readUnsignedShort();
                    int size = walReader.readUnsignedShort();

                    // If it's an UPDATE_PAGE record, skip over the undo data.
                    if (type == WALRecordType.UPDATE_PAGE)
                        walReader.movePosition(size);

                    // Write the redo data into the page.
                    byte[] redoData = new byte[size];
                    walReader.read(redoData);
                    redoPage.write(index, redoData);
                }

                // Finally, the update and redo-only update records store the
                // size of the record (int) and the record type (byte), so
                // skip past them.
                walReader.movePosition(5);

                break;

            default:
                throw new WALFileException(
                    "Encountered unrecognized WAL record type " + type +
                    " at LSN " + currLSN + " during redo processing!");
            }

            oldLSN = currLSN;
            currLSN = computeNextLSN(currLSN.getLogFileNo(), walReader.getPosition());
        }
        
        if (currLSN.compareTo(recoveryInfo.nextLSN) != 0) {
            throw new WALFileException("Traversing WAL file didn't yield " +
                " the same ending LSN as in the transaction-state file.  WAL " +
                " result:  " + currLSN + "  TxnState:  " + recoveryInfo.nextLSN);
        }

        logger.debug("Redo processing is complete.  There are " +
            recoveryInfo.incompleteTxns.size() + " incomplete transactions.");
    }


    private void performUndo(RecoveryInfo recoveryInfo) throws IOException {
        LogSequenceNumber currLSN = recoveryInfo.nextLSN;
        logger.debug("Starting undo processing at LSN " + currLSN);

        LogSequenceNumber oldLSN = null;
        DBFileReader walReader = null;
        while (recoveryInfo.hasIncompleteTxns()) {
            // Compute LSN of previous WAL record.  Start by getting the last
            // byte of the previous WAL record.
            int logFileNo = currLSN.getLogFileNo();
            int fileOffset = currLSN.getFileOffset();

            // Wrap to the previous WAL file if necessary, and if there is one.
            if (fileOffset == OFFSET_FIRST_RECORD) {
                // Need to read the "previous WAL file's last offset" value
                // from the current WAL file.
                walReader = getWALFileReader(currLSN);
                walReader.setPosition(OFFSET_PREV_FILE_END);
                int prevFileEndOffset = walReader.readInt();
                if (prevFileEndOffset == 0) {
                    logger.debug("Reached the very start of the write-ahead log!");
                    break;
                }

                // Need to go back to the previous WAL file.
                logFileNo--;
                if (logFileNo < 0)  // Did we wrap around?
                    logFileNo = MAX_WAL_FILE_NUMBER;

                currLSN = new LogSequenceNumber(logFileNo, prevFileEndOffset);
                fileOffset = currLSN.getFileOffset();
            }
            else if (fileOffset < OFFSET_FIRST_RECORD) {
                // This would be highly unusual, but would indicate either a
                // bug in the undo record-traversal, or a corrupt WAL file.
                throw new WALFileException(String.format("Overshot the start " +
                    "of WAL file %d's records; ended up at file-position %d",
                    logFileNo, fileOffset));
            }

            if (currLSN.compareTo(recoveryInfo.firstLSN) <= 0)
                break;
            
            if (oldLSN == null || oldLSN.getLogFileNo() != logFileNo)
                walReader = getWALFileReader(currLSN);

            // Move backward one byte in the WAL file to read the previous
            // record's type ID.
            walReader.movePosition(-1);
            byte typeID = walReader.readByte();
            WALRecordType type = WALRecordType.valueOf(typeID);

            // Compute the start of the previous record based on its type and
            // other details.
            int startOffset;
            switch (type) {
            case START_TXN:
                // Type (1B) + TransactionID (4B) + Type (1B) = 6 bytes
                startOffset = fileOffset - 6 + 1;
                break;

            case COMMIT_TXN:
            case ABORT_TXN:
                // Type (1B) + TransactionID (4B) + PrevLSN (2B+4B) + Type (1B)
                // = 12 bytes
                startOffset = fileOffset - 12 + 1;
                break;

            case UPDATE_PAGE:
            case UPDATE_PAGE_REDO_ONLY:
                // For these records, the WAL record's start offset is stored
                // immediately before the last type-byte.  We go back 5 bytes
                // because reading the type ID moves the position forward by
                // 1 byte, and then we also have to get to the start of the
                // 4-byte starting offset.
                walReader.movePosition(-5);
                startOffset = walReader.readInt();
                break;

            default:
                throw new WALFileException(
                    "Encountered unrecognized WAL record type " + type +
                        " at LSN " + currLSN + " during redo processing!");
            }

            // Construct a new LSN pointing to the previous record.  If this
            // happens to be before the range that we are using for recovery,
            // we're done with undo-processing.
            currLSN = new LogSequenceNumber(logFileNo, startOffset);
            if (currLSN.compareTo(recoveryInfo.firstLSN) < 0)
                break;

            // Read the transaction ID.  If it's for a completed transaction,
            // we skip over the record.
            int transactionID = walReader.readInt();
            if (recoveryInfo.isTxnComplete(transactionID)) {
                // The current transaction is already completed, so skip the
                // record.
                oldLSN = currLSN;
                continue;
            }

            // Undo specific operations.  Note that we don't have to set the
            // reader's position to anything special at the end of each record,
            // since the above code will always properly move to the appropriate
            // position for the previous record, based on the value of currLSN.

            switch (type) {
                case START_TXN:
                    // Record that the transaction is aborted.
                    writeTxnRecord(WALRecordType.ABORT_TXN, transactionID,
                        recoveryInfo.getLastLSN(transactionID));

                    logger.debug(String.format(
                        "Undo phase:  aborted transaction %d", transactionID));
                    recoveryInfo.recordTxnCompleted(transactionID);

                    break;

                case COMMIT_TXN:
                case ABORT_TXN:
                    // We shouldn't see these records, since this is supposedly
                    // an incomplete transaction!
                    throw new IllegalStateException("Saw a " + type +
                        "WAL-record for supposedly incomplete transaction " +
                        transactionID + "!");

                case UPDATE_PAGE:
                    // Undo the changes to the specified file and page.

                    String undoFilename = walReader.readVarString255();
                    int undoPageNo = walReader.readUnsignedShort();

                    DBFile undoFile = storageManager.openDBFile(undoFilename);
                    DBPage undoPage = storageManager.loadDBPage(undoFile, undoPageNo);

                    // Read the number of segments in the redo/undo record, and
                    // undo the writes.  While we do this, the data for a redo-only
                    // record is also accumulated.

                    int numSegments = walReader.readUnsignedShort();

                    logger.debug(String.format(
                        "Undoing changes to file %s, page %d (%d segments)",
                        undoFile, undoPageNo, numSegments));

                    byte[] redoOnlyData = applyUndoAndGenRedoOnlyData(walReader,
                        undoPage, numSegments);

                    // Update the WAL with the redo-only record.
                    writeRedoOnlyUpdatePageRecord(undoPage, numSegments, redoOnlyData);

                    break;

                case UPDATE_PAGE_REDO_ONLY:
                    // We ignore redo-only updates during the undo phase.
                    break;

                default:
                    throw new WALFileException(
                        "Encountered unrecognized WAL record type " + type +
                            " at LSN " + currLSN + " during undo processing!");
            }

            oldLSN = currLSN;
        }

        logger.debug("Undo processing is complete.");
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
            fileOffset = OFFSET_FIRST_RECORD;
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
            // TODO:  Write the previous WAL file's last file-offset into the new WAL file's start.
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
     * state is passed explicitly so that this method can be used during
     * recovery processing.  The alternate method
     * {@link #writeTxnRecord(WALRecordType)} retrieves the transaction state
     * from thread-local storage, and should be used during normal operation.
     *
     * @param type The type of the transaction demarcation to write, one of the
     *        values {@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN},
     *        or {@link WALRecordType#ABORT_TXN}.
     *
     * @param transactionID the transaction ID that the WAL record is for
     *
     * @param prevLSN the log sequence number of the transaction's immediately
     *        previous WAL record, if the record type is either a commit or
     *        abort record.
     *                      
     * @throws IOException if the write-ahead log can't be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>type</tt> is <tt>null</tt>, or if
     *         it isn't one of the values {@link WALRecordType#START_TXN},
     *         {@link WALRecordType#COMMIT_TXN}, or {@link WALRecordType#ABORT_TXN}.
     */
    public synchronized void writeTxnRecord(WALRecordType type,
        int transactionID, LogSequenceNumber prevLSN) throws IOException {

        if (type != WALRecordType.START_TXN &&
            type != WALRecordType.COMMIT_TXN &&
            type != WALRecordType.ABORT_TXN) {
            throw new IllegalArgumentException("Invalid record type " + type +
                " passed to writeTxnRecord().");
        }

        if ((type == WALRecordType.COMMIT_TXN ||
             type == WALRecordType.ABORT_TXN) && prevLSN == null) {
            throw new IllegalArgumentException(
                "prevLSN must be specified for records of type " + type);
        }

        logger.debug("Writing a " + type + " record for transaction " +
            transactionID + " at LSN " + nextLSN);

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(nextLSN);

        walWriter.writeByte(type.getID());
        walWriter.writeInt(transactionID);

        if (type == WALRecordType.START_TXN) {
            walWriter.writeByte(type.getID());
        }
        else {
            walWriter.writeShort(prevLSN.getLogFileNo());
            walWriter.writeInt(prevLSN.getFileOffset());
            walWriter.writeByte(type.getID());
        }

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
        logger.debug("Next-LSN value is now " + nextLSN);

        // In the case of committing, we need to sync the write-ahead log file
        // to the disk so that we can actually report the transaction as
        // "committed".  We do this after updating the nextLSN value so that we
        // can guarantee that the *entire* commit-record is written to disk.
        if (type == WALRecordType.COMMIT_TXN) {
            // Sync the write-ahead log to disk, up to the LSN of the commit
            // record in the WAL.
            storageManager.forceWAL(nextLSN);
        }

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

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        writeTxnRecord(type, txnState.getTransactionID(), txnState.getLastLSN());
        txnState.setLastLSN(nextLSN);
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

        // Store the LSN of the change on the page.
        nextLSN.setRecordSize(walWriter.getPosition() - nextLSN.getFileOffset());
        dbPage.setPageLSN(nextLSN);

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
        txnState.setLastLSN(nextLSN);
    }


    private byte[] applyUndoAndGenRedoOnlyData(DBFileReader walReader,
        DBPage dbPage, int numSegments) throws IOException {

        NoCopyByteArrayOutputStream redoOnlyBAOS =
            new NoCopyByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(redoOnlyBAOS);

        for (int i = 0; i < numSegments; i++) {
            // Apply the undo data to the data page.
            int start = walReader.readUnsignedShort();
            int length = walReader.readUnsignedShort();

            byte[] undoData = new byte[length];
            walReader.read(undoData);
            dbPage.write(start, undoData);

            // Record what we wrote into the redo-only record data.
            dos.writeShort(start);
            dos.writeShort(length);
            dos.write(undoData);
        }

        // Return the data that will appear in the redo-only record body.
        dos.flush();
        return redoOnlyBAOS.getBuf();
    }


    /**
     * This method writes a redo-only update-page record to the write-ahead log,
     * including only redo details.  The transaction state is passed explicitly
     * so that this method can be used during recovery processing.  The
     * alternate method
     * {@link #writeRedoOnlyUpdatePageRecord(DBPage, int, byte[])} retrieves
     * the transaction state from thread-local storage, and should be used
     * during normal operation.
     *
     * @param transactionID the transaction ID that the WAL record is for.
     *
     * @param prevLSN the log sequence number of the transaction's immediately
     *        previous WAL record.
     *
     * @param dbPage The data page whose changes are to be recorded in the log.
     *
     * @param numSegments The number of segments in the change-data to record.
     *
     * @param changes The actual changes themselves, serialized to a byte array.

     * @throws IOException if the write-ahead log cannot be updated for some
     *         reason.
     *
     * @throws IllegalArgumentException if <tt>dbPage</tt> is <tt>null</tt>, or
     *         if <tt>changes</tt> is <tt>null</tt>.
     */
    public synchronized void writeRedoOnlyUpdatePageRecord(int transactionID,
        LogSequenceNumber prevLSN, DBPage dbPage, int numSegments,
        byte[] changes) throws IOException {

        if (dbPage == null)
            throw new IllegalArgumentException("dbPage must be specified");

        if (changes == null)
            throw new IllegalArgumentException("changes must be specified");

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFileWriter walWriter = getWALFileWriter(nextLSN);

        walWriter.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());
        walWriter.writeInt(transactionID);

        // We need to store the previous log sequence number for this record.
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

        // Store the LSN of the change on the page.
        nextLSN.setRecordSize(walWriter.getPosition() - nextLSN.getFileOffset());
        dbPage.setPageLSN(nextLSN);

        nextLSN = computeNextLSN(nextLSN.getLogFileNo(), walWriter.getPosition());
    }
    
    
    public void writeRedoOnlyUpdatePageRecord(DBPage dbPage, int numSegments,
                                              byte[] changes) throws IOException {

        // Retrieve and verify the transaction state.
        TransactionState txnState = SessionState.get().getTxnState();
        if (!txnState.isTxnInProgress()) {
            throw new IllegalStateException(
                "No transaction is currently in progress!");
        }

        writeRedoOnlyUpdatePageRecord(txnState.getTransactionID(),
            txnState.getLastLSN(), dbPage, numSegments, changes);

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
                throw new WALFileException(String.format("Sent to WAL record " +
                    "for transaction %d at LSN %s, during rollback of " +
                    "transaction %d.", recordTxnID, lsn, transactionID));
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

                // Open the specified file and retrieve the data page to undo.
                DBFile dbFile = storageManager.openDBFile(filename);
                DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);

                // Read the number of segments in the redo/undo record, and
                // undo the writes.  While we do this, the data for a redo-only
                // record is also accumulated.
                int numSegments = walReader.readUnsignedShort();
                byte[] redoOnlyData = applyUndoAndGenRedoOnlyData(walReader,
                    dbPage, numSegments);

                // Finally, update the WAL with the redo-only record.
                writeRedoOnlyUpdatePageRecord(dbPage, numSegments, redoOnlyData);
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
