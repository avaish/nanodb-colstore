package edu.caltech.nanodb.storage.writeahead;


import edu.caltech.nanodb.util.RegexFileFilter;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.storage.DBFile;
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


    /** The current WAL file being logged to. */
    DBFile walFile;


    /**
     * This object holds the log sequence number where the next write-ahead log
     * record will be written.
     */
    LogSequenceNumber nextLSN;


    private StorageManager storageManager;


    public WALManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    public void init() {

    }
    
    
    private void updateNextLSN(int fileNo, int fileOffset) {
        if (fileOffset >= MAX_WAL_FILE_SIZE) {
            // This WAL file has reached the size limit.  Increment the file
            // number, wrapping around if necessary, and reset the offset to 0.
            fileNo += 1;
            if (fileNo > MAX_WAL_FILE_NUMBER)
                fileNo = 0;

            fileOffset = 0;
        }
        nextLSN = new LogSequenceNumber(fileNo, fileOffset);
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
    private DBFile getWALFile(LogSequenceNumber lsn)
        throws IOException {

        int fileNo = lsn.getLogFileNo();
        int offset = lsn.getFileOffset();

        DBFile walFile = storageManager.openWALFile(fileNo);
        RandomAccessFile contents = walFile.getFileContents();
        contents.seek(offset);

        return walFile;
    }


    /**
     * This function writes a transaction demarcation record
     * ({@link WALRecordType#START_TXN}, {@link WALRecordType#COMMIT_TXN}, or
     * {@link WALRecordType#ABORT_TXN}) to the write-ahead log.
     *
     * @param type
     * @throws IOException
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

        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFile walFile = getWALFile(nextLSN);
        RandomAccessFile contents = walFile.getFileContents();

        contents.writeByte(type.getID());
        contents.writeInt(txnState.getTransactionID());
        
        if (type == WALRecordType.START_TXN) {
            contents.writeByte(type.getID());
        }
        else {
            LogSequenceNumber prevLSN = txnState.getLastLSN();
            contents.writeShort(prevLSN.getLogFileNo());
            contents.writeInt(prevLSN.getFileOffset());
            contents.writeByte(type.getID());
        }

        updateNextLSN(nextLSN.getLogFileNo(), (int) contents.getFilePointer());

        // In the case of committing, we need to sync the write-ahead log file
        // to the disk so that we can actually report the transaction as
        // "committed".
        if (type == WALRecordType.COMMIT_TXN) {
            // Sync the write-ahead log to disk.
            storageManager.syncWALFile(walFile);

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
        
        // Record the WAL record.  First thing to do:  figure out where it goes.

        DBFile walFile = getWALFile(nextLSN);
        RandomAccessFile contents = walFile.getFileContents();

        contents.writeByte(WALRecordType.UPDATE_PAGE.getID());
        contents.writeInt(txnState.getTransactionID());

        // We need to store the previous log sequence number for this record.
        LogSequenceNumber prevLSN = txnState.getLastLSN();
        contents.writeShort(prevLSN.getLogFileNo());
        contents.writeInt(prevLSN.getFileOffset());

        contents.writeUTF(dbPage.getDBFile().getDataFile().getName());
        contents.writeShort(dbPage.getPageNo());

        // This offset is where we will store the number of data segments we
        // need to record.
        int segCountOffset = (int) contents.getFilePointer();
        contents.writeShort(-1);
        
        byte[] oldData = dbPage.getOldPageData();
        byte[] newData = dbPage.getPageData();
        int pageSize = dbPage.getPageSize();
        int numSegments = 0;
        int index = 0;
        while (index < pageSize) {
            // Skip data until we find stuff that's different.
            index += ArrayUtil.sizeOfIdenticalRange(oldData, newData, index);
            if (index >= pageSize)
                break;

            // Find out how much data is actually changed.  We lump in small
            // runs of unchanged data just to simplify things.
            int size = ArrayUtil.sizeOfDifferentRange(oldData, newData, index);
            while (index + size < pageSize) {
                while (index + size < pageSize) {
                    int sameSize = ArrayUtil.sizeOfIdenticalRange(
                        oldData, newData, index + size);

                    if (sameSize >= 4)
                        break;
                    
                    size += sameSize;
                }

                // Write the starting index within the page, and the amount of
                // data that will be recorded at that index.
                contents.writeShort(index);
                contents.writeShort(size);

                // Write the old data (undo), and then the new data (redo).
                contents.write(oldData, index, size);
                contents.write(newData, index, size);

                numSegments++;
            }

            index += size;
        }
        
        // Now that we know how many segments were recorded, store that value
        // at the appropriate location.
        int currOffset = (int) contents.getFilePointer();
        contents.seek(segCountOffset);
        contents.writeShort(numSegments);
        contents.seek(currOffset);
        
        // Write the start of the update record at the end so that we can get
        // back to the record's start when scanning the log backwards.

        contents.writeInt(nextLSN.getFileOffset());
        contents.writeByte(WALRecordType.UPDATE_PAGE.getID());

        updateNextLSN(nextLSN.getLogFileNo(), (int) contents.getFilePointer());
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

        DBFile walFile = getWALFile(nextLSN);
        RandomAccessFile contents = walFile.getFileContents();

        contents.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());
        contents.writeInt(txnState.getTransactionID());

        // We need to store the previous log sequence number for this record.
        LogSequenceNumber prevLSN = txnState.getLastLSN();
        contents.writeShort(prevLSN.getLogFileNo());
        contents.writeInt(prevLSN.getFileOffset());

        contents.writeUTF(dbPage.getDBFile().getDataFile().getName());
        contents.writeShort(dbPage.getPageNo());

        // Write the redo-only data.
        contents.writeShort(numSegments);
        contents.write(changes);

        // Write the start of the update record at the end so that we can get
        // back to the record's start when scanning the log backwards.

        contents.writeInt(nextLSN.getFileOffset());
        contents.writeByte(WALRecordType.UPDATE_PAGE_REDO_ONLY.getID());

        updateNextLSN(nextLSN.getLogFileNo(), (int) contents.getFilePointer());
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
            DBFile walFile = getWALFile(lsn);
            RandomAccessFile contents = walFile.getFileContents();

            WALRecordType type = WALRecordType.valueOf(contents.readByte());
            int recordTxnID = contents.readInt();
            if (recordTxnID != transactionID) {
                throw new RuntimeException(
                    "FATAL ERROR:  Write-ahead log is corrupt!");
            }

            if (type == WALRecordType.START_TXN) {
                // Done rolling back the transaction.
                break;
            }

            // Read out the "previous LSN" value.
            int prevFileNo = contents.readUnsignedShort();
            int prevOffset = contents.readInt();
            LogSequenceNumber prevLSN =
                new LogSequenceNumber(prevFileNo, prevOffset);

            if (type == WALRecordType.UPDATE_PAGE) {
                // Undo this change.

                // Read the file and page with the changes to undo.
                String filename = contents.readUTF();
                int pageNo = contents.readUnsignedShort();

                int numSegments = contents.readUnsignedShort();

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
                    int start = contents.readUnsignedShort();
                    int length = contents.readUnsignedShort();
                    contents.readFully(data, start, length);

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
        }

        // All done rolling back the transaction!  Record that it was aborted
        // in the WAL.
        writeTxnRecord(WALRecordType.ABORT_TXN);
        logger.info(String.format("Transaction %d:  Rollback complete.",
            transactionID));
    }


    public List<File> findWALFiles() {
        File[] walFiles = storageManager.getBaseDir().listFiles(
            new RegexFileFilter(WAL_FILENAME_REGEX));

        TreeMap<Integer, File> orderedFiles = new TreeMap<Integer, File>();
        Matcher m = Pattern.compile(WAL_FILENAME_REGEX).matcher("");
        for (File f : walFiles) {
            m.reset(f.getName());
            if (!m.matches()) {
                logger.error("Matched non-WAL file " + f);
                continue;
            }
            
            int n = Integer.parseInt(m.group(1));
            orderedFiles.put(n, f);
        }
        
        return new ArrayList<File>(orderedFiles.values());
    }


    public void performRecovery() {
        List<File> walFiles = findWALFiles();

        Set<Integer> incompleteTxns = performRedo(walFiles);
        performUndo(walFiles, incompleteTxns);
    }


    private Set<Integer> performRedo(List<File> walFiles) {
        HashSet<Integer> incompleteTxns = new HashSet<Integer>();

        // TODO:  Perform redo processing.

        return incompleteTxns;
    }


    private void performUndo(List<File> walFiles, Set<Integer> incompleteTxns) {
        // TODO:  Perform undo processing on incomplete transactions.
    }
}
