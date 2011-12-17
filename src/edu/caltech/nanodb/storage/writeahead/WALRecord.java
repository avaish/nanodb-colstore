package edu.caltech.nanodb.storage.writeahead;


import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.storage.FilePointer;


/**
 * This class represents a record in the Write-Ahead Log.
 */
public class WALRecord {
    /** The type of the WAL record. */
    WALRecordType type;

    /** This is the transaction ID that the WAL record corresponds to. */
    int txnID;

    /**
     * If the WAL record's type is {@link WALRecordType#UPDATE_ATTRS},
     * this is the name of the table being modified.  Otherwise, this value is
     * unspecified.
     */
    String tableName;

    /**
     * If the WAL record's type is {@link WALRecordType#UPDATE_ATTRS},
     * this is the file-pointer of the tuple being modified.  Otherwise, this
     * value is unspecified.
     */
    FilePointer tuplePtr;

    /**
     * If the WAL record's type is {@link WALRecordType#UPDATE_ATTRS},
     * this is the index of the attribute being modified.  Otherwise, this value
     * is unspecified.
     */
    int attrIndex;

    /**
     * If the WAL record's type is {@link WALRecordType#UPDATE_ATTRS},
     * this is the type of the attribute being modified.  Otherwise, this value
     * is unspecified.
     */
    SQLDataType attrType;

    /**
     * If the WAL record's type is {@link WALRecordType#UPDATE_ATTRS}
     * this is the old value of the attribute being modified.  Otherwise, this
     * value is unspecified.
     */
    Object oldValue;

    /**
     * If the WAL record's type is {@link WALRecordType#UPDATE_ATTRS}
     * this is the new value of the attribute being modified.  Otherwise, this
     * value is unspecified.
     */
    Object newValue;
}
