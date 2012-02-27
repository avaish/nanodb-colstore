package edu.caltech.nanodb.transactions;


import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;


/**
 * This class wraps the checkpoint page to provide basic operations necessary
 * for reading and storing essential values.
 */
public class TransactionStatePage {

    /**
     * The offset in the checkpoint page where the "Next Transaction ID" value
     * is stored.  This value is a signed int (4 bytes).
     */
    public static final int OFFSET_NEXT_TXN_ID = 2;


    /**
     * The offset in the checkpoint page where the "First Log Sequence Number"
     * file-number is stored.  This value is an unsigned short (2 bytes).
     */
    public static final int OFFSET_FIRST_LSN_FILENUM = 6;


    /**
     * The offset in the checkpoint page where the "First Log Sequence Number"
     * file-offset is stored.  This value is a signed int (4 bytes).
     */
    public static final int OFFSET_FIRST_LSN_OFFSET = 8;


    /**
     * The offset in the checkpoint page where the "Next Log Sequence Number"
     * file-number is stored.  This value is an unsigned short (2 bytes).
     */
    public static final int OFFSET_NEXT_LSN_FILENUM = 12;

    /**
     * The offset in the checkpoint page where the "Next Log Sequence Number"
     * file-offset is stored.  This value is a signed int (4 bytes).
     */
    public static final int OFFSET_NEXT_LSN_OFFSET = 14;


    private DBPage dbPage;


    public TransactionStatePage(DBPage dbPage) {
        this.dbPage = dbPage;
    }


    public int getNextTransactionID() {
        return dbPage.readInt(OFFSET_NEXT_TXN_ID);
    }


    public void setNextTransactionID(int nextTransactionID) {
        dbPage.writeInt(OFFSET_NEXT_TXN_ID, nextTransactionID);
    }


    public LogSequenceNumber getFirstLSN() {
        int fileNum = dbPage.readUnsignedShort(OFFSET_FIRST_LSN_FILENUM);
        int offset = dbPage.readInt(OFFSET_FIRST_LSN_OFFSET);

        return new LogSequenceNumber(fileNum, offset);
    }


    public void setFirstLSN(LogSequenceNumber firstLSN) {
        dbPage.writeShort(OFFSET_FIRST_LSN_FILENUM, firstLSN.getLogFileNo());
        dbPage.writeInt(OFFSET_FIRST_LSN_OFFSET, firstLSN.getFileOffset());
    }


    public LogSequenceNumber getNextLSN() {
        int fileNum = dbPage.readUnsignedShort(OFFSET_NEXT_LSN_FILENUM);
        int offset = dbPage.readInt(OFFSET_NEXT_LSN_OFFSET);

        return new LogSequenceNumber(fileNum, offset);
    }


    public void setNextLSN(LogSequenceNumber nextLSN) {
        dbPage.writeShort(OFFSET_NEXT_LSN_FILENUM, nextLSN.getLogFileNo());
        dbPage.writeInt(OFFSET_NEXT_LSN_OFFSET, nextLSN.getFileOffset());
    }
}
