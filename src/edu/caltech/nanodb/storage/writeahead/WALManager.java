package edu.caltech.nanodb.storage.writeahead;


import com.sun.xml.internal.bind.v2.TODO;
import edu.caltech.nanodb.commands.UpdateCommand;
import edu.caltech.nanodb.storage.PageWriter;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import edu.caltech.nanodb.client.SessionState;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionState;


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


    public synchronized void writeTxnRecord(int txnID, WALRecordType type)
        throws IOException {

        if (txnID < 0) {
            throw new IllegalArgumentException("Transaction ID " + txnID +
                " is invalid");
        }

        if (type != WALRecordType.START_TXN &&
            type != WALRecordType.COMMIT_TXN &&
            type != WALRecordType.ABORT_TXN) {
            throw new IllegalArgumentException("Invalid record type " + type +
                " passed to writeTxnRecord().");
        }

        // Record the WAL record.  First thing to do:  figure out where it goes.

        int fileNo = nextLSN.getLogFileNo();
        int pageNo = nextLSN.getPageNo();
        int offset = nextLSN.getOffset();

        DBFile dbFile = storageManager.openWALFile(fileNo);

        // We need to store the previous log sequence number for this record.
        TransactionState txnState = SessionState.get().getTxnState();

        LogSequenceNumber prevLSN = null;
        if (type == WALRecordType.COMMIT_TXN || type == WALRecordType.ABORT_TXN) {
            prevLSN = txnState.getLastLSN();
        }

        int recordSize = 12;
        if (type == WALRecordType.START_TXN)
            recordSize = 6;

        if (dbFile.getPageSize() - offset < recordSize) {
            // Need to advance to the next data page.
            // TODO:  Move on to the next WAL file if necessary.
            pageNo++;
            offset = 0;
        }
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo, true);

        dbPage.writeByte(offset, type.getID());
        dbPage.writeInt(offset + 1, txnState.getTransactionID());
        
        if (type == WALRecordType.START_TXN) {
            dbPage.writeByte(offset + 5, type.getID());
        }
        else {
            dbPage.writeShort(offset + 5, prevLSN.getLogFileNo());
            dbPage.writeShort(offset + 7, prevLSN.getPageNo());
            dbPage.writeShort(offset + 9, prevLSN.getOffset());
            dbPage.writeByte(offset + 11, type.getID());
        }
        offset += recordSize;
        
        nextLSN = new LogSequenceNumber(fileNo, pageNo, offset);

        // In the case of committing, we need to sync the write-ahead log file
        // to the disk so that we can actually report the transaction as
        // "committed".
        if (type == WALRecordType.COMMIT_TXN) {
            // TODO:  Sync the write-ahead log to disk.
            storageManager.syncWALFile(dbFile);

            // TODO:  This should probably be done by the transaction manager.
            txnState.clear();
        }
        else {
            txnState.setLastLSN(nextLSN);
        }
    }


    public synchronized void writeUpdatePage(int txnID, DBPage dbPage)
        throws IOException {
        writeUpdatePage(txnID, dbPage, false);
    }

    
    public synchronized void writeUpdatePage(int txnID, DBPage dbPage,
        boolean redoOnly) throws IOException {

        if (txnID < 0) {
            throw new IllegalArgumentException("Transaction ID " + txnID +
                " is invalid");
        }

        if (dbPage == null)
            throw new IllegalArgumentException("dbPage must be specified");

        if (!dbPage.isDirty())
            throw new IllegalArgumentException("dbPage has no updates to store");

        // Record the WAL record.
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(baos));

        String filename = dbPage.getDBFile().getDataFile().getName();
        dos.writeUTF(filename);

        byte[] oldData = dbPage.getOldPageData();
        byte[] newData = dbPage.getPageData();
        int pageSize = dbPage.getPageSize();
        int index = 0;
        while (index < pageSize) {
            // Skip data until we find stuff that's different.
            index += sizeOfIdenticalRange(oldData, newData, index);
            if (index >= pageSize)
                break;

            // Find out how much data is actually changed.  We lump in small
            // runs of unchanged data just to simplify things.
            int size = sizeOfDifferentRange(oldData, newData, index);
            while (index + size < pageSize) {
                while (index + size < pageSize) {
                    int sameSize =
                        sizeOfIdenticalRange(oldData, newData, index + size);

                    if (sameSize >= 8)
                        break;
                    
                    size += sameSize;
                }

                dos.writeShort(index);
                dos.writeShort(size);

                if (!redoOnly)
                    dos.write(oldData, index, size);

                dos.write(newData, index, size);
            }

            index += size;
        }

        dos.write(dbPage.getPageData());
        dos.close();

        byte[] data = baos.toByteArray();

        // Record the WAL record.  First thing to do:  figure out where it goes.

        int fileNo = nextLSN.getLogFileNo();
        int pageNo = nextLSN.getPageNo();
        int offset = nextLSN.getOffset();

        DBFile dbFile = storageManager.openWALFile(fileNo);

        // We need to store the previous log sequence number for this record.
        TransactionState txnState = SessionState.get().getTxnState();

        LogSequenceNumber prevLSN = txnState.getLastLSN();

        WALRecordType type = (redoOnly ? WALRecordType.UPDATE_PAGE_REDO_ONLY :
                                         WALRecordType.UPDATE_PAGE);

        if (dbFile.getPageSize() - offset < 11) {
            // Need to advance to the next data page.
            // TODO:  Move on to the next WAL file if necessary.
            pageNo++;
            offset = 0;
        }
        DBPage walPage = storageManager.loadDBPage(walFile, pageNo, true);

        walPage.writeByte(offset, type.getID());
        walPage.writeInt(offset + 1, txnState.getTransactionID());
        walPage.writeShort(offset + 5, prevLSN.getLogFileNo());
        walPage.writeShort(offset + 7, prevLSN.getPageNo());
        walPage.writeShort(offset + 9, prevLSN.getOffset());
        walPage.writeByte(offset + 11, type.getID());

        ///// TODO:  Write actual data, and the payload size as well.

        nextLSN = new LogSequenceNumber(fileNo, pageNo, offset);

        ///// TODO:  Update the transaction state in the session.
    }

    
    private int sizeOfIdenticalRange(byte[] a, byte[] b, int index) {
        if (a == null)
            throw new IllegalArgumentException("a must be specified");

        if (b == null)
            throw new IllegalArgumentException("b must be specified");

        if (a.length != b.length)
            throw new IllegalArgumentException("a and b must be the same size");
        
        if (index < 0 || index >= a.length) {
            throw new IllegalArgumentException(
                "off must be a valid index into the arrays");
        }

        int size = 0;
        for (int i = index; i < a.length && a[i] == b[i]; i++, size++);

        return size;
    }


    private int sizeOfDifferentRange(byte[] a, byte[] b, int index) {
        if (a == null)
            throw new IllegalArgumentException("a must be specified");

        if (b == null)
            throw new IllegalArgumentException("b must be specified");

        if (a.length != b.length)
            throw new IllegalArgumentException("a and b must be the same size");

        if (index < 0 || index >= a.length) {
            throw new IllegalArgumentException(
                "off must be a valid index into the arrays");
        }

        int size = 0;
        for (int i = index; i < a.length && a[i] != b[i]; i++, size++);

        return size;
    }

    

    public void performRecovery() {
        Set<Integer> incompleteTxns = performRedo();
        performUndo(incompleteTxns);
    }


    private Set<Integer> performRedo() {
        HashSet<Integer> incompleteTxns = new HashSet<Integer>();

        // TODO:  Perform redo processing.

        return incompleteTxns;
    }


    private void performUndo(Set<Integer> incompleteTxns) {
        // TODO:  Perform undo processing on incomplete transactions.
    }
}
