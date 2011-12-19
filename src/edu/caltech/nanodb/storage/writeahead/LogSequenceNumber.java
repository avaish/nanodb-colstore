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
public class LogSequenceNumber
    implements Comparable<LogSequenceNumber>, Cloneable {

    /** The number of the write-ahead log file that the record is stored in. */
    private int logFileNo;

    /** The offset of the log record from the start of the file. */
    private int fileOffset;

    
    public LogSequenceNumber(int logFileNo, int fileOffset) {
        if (logFileNo < 0 || logFileNo > WALManager.MAX_WAL_FILE_NUMBER) {
            throw new IllegalArgumentException(String.format(
                "WAL file numbers must be in the range [0, %d]; got %d instead.",
                WALManager.MAX_WAL_FILE_NUMBER, logFileNo));
        }

        if (fileOffset < 0)
            throw new IllegalArgumentException("File offset must be nonnegative");

        this.logFileNo = logFileNo;
        this.fileOffset = fileOffset;
    }
    
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LogSequenceNumber) {
            LogSequenceNumber lsn = (LogSequenceNumber) obj;
            return lsn.logFileNo == logFileNo && lsn.fileOffset == fileOffset;
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
        hashCode = 53 * hashCode + fileOffset;

        return hashCode;
    }
    

    public LogSequenceNumber clone() {
        // Make a deep copy of the entire object; this means also cloning the
        // file-pointer.
        try {
            LogSequenceNumber lsn = (LogSequenceNumber) super.clone();
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

    
    public int getFileOffset() {
        return fileOffset;
    }


    @Override
    public int compareTo(LogSequenceNumber lsn) {
        if (logFileNo != lsn.logFileNo)
            return logFileNo - lsn.logFileNo;

        return fileOffset - lsn.fileOffset;
    }
    
    
    @Override
    public String toString() {
        return String.format("LSN[%06d:%08d]", logFileNo, fileOffset);
    }
}
