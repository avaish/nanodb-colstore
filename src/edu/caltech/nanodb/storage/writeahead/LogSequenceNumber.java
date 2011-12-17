package edu.caltech.nanodb.storage.writeahead;


import edu.caltech.nanodb.storage.FilePointer;
import org.apache.commons.lang.ObjectUtils;


/**
 * This class represents a Log Sequence Number (LSN) in the write-ahead log.
 * Every log record has a unique LSN value; furthermore, the LSN identifies the
 * exact location of the log record on disk as well.  Log Sequence Numbers are
 * comprised of the following parts:
 *
 * <ul>
 *   <li>The file number of the write-ahead log file for the record (range:
 *       000000..999999)</li>
 *   <li>The page within the write-ahead log file (range:  0..65535)</li>
 *   <li>The offset within the page (range:  0..65535)</li>
 * </ul>
 */
public class LogSequenceNumber implements Cloneable {
    private int logFileNo;


    private FilePointer filePointer;

    
    public LogSequenceNumber(int logFileNo, FilePointer fptr) {
        if (logFileNo < 0 || logFileNo > WALManager.MAX_WAL_FILE_NUMBER) {
            throw new IllegalArgumentException(String.format(
                "WAL file numbers must be in the range [0, %d]; got %d instead.",
                WALManager.MAX_WAL_FILE_NUMBER, logFileNo));
        }

        if (fptr == null)
            throw new IllegalArgumentException("File pointer must be non-null");

        this.logFileNo = logFileNo;
        this.filePointer = fptr;
    }
    
    
    public LogSequenceNumber(int logFileNo, int pageNo, int offset) {
        this(logFileNo, new FilePointer(pageNo, offset));
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LogSequenceNumber) {
            LogSequenceNumber lsn = (LogSequenceNumber) obj;
            return lsn.logFileNo == logFileNo &&
                ObjectUtils.equals(lsn.filePointer, filePointer);
        }
        return false;
    }


    /** Calculate a hash-code for this log-sequence number object. */
    @Override
    public int hashCode() {
        int hashCode;

        // Follows pattern in Effective Java, Item 8, with different constants.
        hashCode = 37;
        hashCode = 53 * hashCode + logFileNo;
        hashCode = 53 * hashCode +
            (filePointer != null ? filePointer.hashCode() : 0);

        return hashCode;
    }
    

    public LogSequenceNumber clone() {
        // Make a deep copy of the entire object; this means also cloning the
        // file-pointer.
        try {
            LogSequenceNumber lsn = (LogSequenceNumber) super.clone();
            lsn.filePointer = (FilePointer) filePointer.clone();

            return lsn;
        }
        catch (CloneNotSupportedException e) {
            // This is completely unexpected, so wrap it in a RuntimeException
            // and rethrow it.
            throw new RuntimeException(e.getMessage(), e);
        }
    }
    
    
    public int getLogFileNo() {
        return logFileNo;
    }

    
    public FilePointer getFilePointer() {
        return filePointer;
    }
    
    
    public int getPageNo() {
        return filePointer.getPageNo();
    }
    
    
    public int getOffset() {
        return filePointer.getOffset();
    }
}
