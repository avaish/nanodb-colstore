package edu.caltech.nanodb.storage.btreeindex;


import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexPointer;
import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.PageTuple;

import java.util.ArrayList;
import java.util.List;


/**
 */
public class LeafPage {
    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;


    /** The page in the database file where the previous leaf is stored. */
    public static final int OFFSET_PREV_PAGE_NO = 1;


    /** The page in the database file where the next leaf is stored. */
    public static final int OFFSET_NEXT_PAGE_NO = 3;


    /**
     * The number of pointer entries in the page.  Note that this value is the
     * number of keys + 1.
     */
    public static final int OFFSET_NUM_ENTRIES = 5;


    /**
     * The offset of the first file-pointer in the leaf-page.  In the leaf
     * pages, each pointer includes both a page number and an offset, so these
     * are 4 bytes each.
     */
    public static final int OFFSET_FIRST_POINTER = 7;


    /**
     * The offset of the first key in the leaf-page.  In the leaf pages, each
     * pointer includes both a page number and an offset, so these are 4 bytes
     * each.
     */
    public static final int OFFSET_FIRST_KEY = 11;

    
    private DBPage dbPage;
    
    
    private List<ColumnInfo> colInfos;
    
    
    /** The number of entries (pointers + keys) stored within this leaf page. */
    private int numEntries;


    /** A list of the pointers stored in this leaf page. */
    private ArrayList<IndexPointer> pointers;


    /** A list of the keys stored in this leaf page. */
    private ArrayList<BTreeIndexPageTuple> keys;


    /**
     * The total size of all data (pointers + keys + initial values) stored
     * within this leaf page.  This is also the offset at which we can start
     * writing more data without overwriting anything.
     */
    private int dataSize;


    public LeafPage(DBPage dbPage, List<ColumnInfo> colInfos) {
        this.dbPage = dbPage;
        this.colInfos = colInfos;

        loadPageContents();
    }


    private void loadPageContents() {
        numEntries = dbPage.readUnsignedShort(OFFSET_NUM_ENTRIES);
        pointers = new ArrayList<IndexPointer>(numEntries);
        keys = new ArrayList<BTreeIndexPageTuple>(numEntries);

        if (numEntries > 0) {
            // Handle first key separately since we know its offset.

            BTreeIndexPageTuple key =
                new BTreeIndexPageTuple(dbPage, OFFSET_FIRST_KEY, colInfos);

            keys.add(key);
            pointers.add(getPointerBeforeKey(key));

            // Handle remaining keys.
            for (int i = 1; i < numEntries; i++) {
                key = new BTreeIndexPageTuple(dbPage, key.getEndOffset() + 4,
                                              colInfos);
                keys.add(key);
                pointers.add(getPointerBeforeKey(key));
            }

            dataSize = key.getEndOffset();
        }
        else {
            // There are no entries (pointers + keys).
            dataSize = OFFSET_FIRST_POINTER;
        }
    }
    
    
    public int getPrevPageNo() {
        return dbPage.readUnsignedShort(OFFSET_PREV_PAGE_NO);
    }


    public int getNextPageNo() {
        return dbPage.readUnsignedShort(OFFSET_NEXT_PAGE_NO);
    }


    public int getNumEntries() {
        return numEntries;
    }
    
    
    public int getDataSize() {
        return dataSize;
    }


    public int getFreeSpace() {
        return dbPage.getPageSize() - dataSize;
    }

    
    public BTreeIndexPageTuple getKey(int index) {
        return keys.get(index);
    }


    public IndexPointer getPointerBeforeKey(BTreeIndexPageTuple tup) {
        // In leaf pages, there are the same number of pointers and keys.  Each
        // key is preceded by a file-pointer consisting of a page number and an
        // offset within the page.  There is no pointer after the last key.
        if (tup.getDBPage() != dbPage) {
            throw new IllegalArgumentException(
                "Key is from a different page than this page.");
        }
            
        int pageNo = dbPage.readUnsignedShort(tup.getOffset() - 4);
        int offset = dbPage.readUnsignedShort(tup.getOffset() - 2);
        return new IndexPointer(pageNo, offset);
    }


    /**
     * Compares two keys and their corresponding tuple-pointers, and a value is
     * returned to indicate the ordering:
     * <ul>
     *   <li>Result &lt; 0 if <tt>tuple[colIndexes]</tt> &lt; <tt>keyTuple</tt></li>
     *   <li>Result == 0 if <tt>tuple[colIndexes]</tt> == <tt>keyTuple</tt></li>
     *   <li>Result &gt; 0 if <tt>tuple[colIndexes]</tt> &gt; <tt>keyTuple</tt></li>
     * </ul>
     *
     * @return a negative, positive, or zero value indicating the ordering of
     *         the two inputs
     */
    @SuppressWarnings("unchecked")
    public static int compareToKey(Tuple keyA, FilePointer tuplePtrA,
                                   Tuple keyB, FilePointer tuplePtrB) {

        if (keyA.getColumnCount() != keyB.getColumnCount())
            throw new IllegalArgumentException("keys must be the same size");

        int compareResult = 0;

        int size = keyA.getColumnCount();
        for (int i = 0; i < size; i++) {
            Comparable valueA = (Comparable) keyA.getColumnValue(i);
            Comparable valueB = (Comparable) keyB.getColumnValue(i);

            // Although it should be "unknown" when we compare two NULL values
            // for equality, we say they are equal so that they will all appear
            // together in the sorting results.
            if (valueA == null) {
                if (valueB != null)
                    compareResult = -1;
                else
                    compareResult = 0;
            }
            else if (valueB == null) {
                compareResult = 1;
            }
            else {
                compareResult = valueA.compareTo(valueB);
            }
        }

        if (compareResult == 0 && tuplePtrA != null && tuplePtrB != null) {
            // Compare the file-pointers as well.
            compareResult = tuplePtrA.compareTo(tuplePtrB);
        }

        return compareResult;
    }


    public IndexPointer addEntryAtIndex(TupleLiteral newKey,
                                        FilePointer newFilePtr, int index) {
        // Get the length of the new tuple, and add in the size of the
        // file-pointer as well.
        int len = PageTuple.getTupleStorageSize(colInfos, newKey) + 4;

        BTreeIndexPageTuple key = getKey(index);

        // Make space for the new key/pointer to be stored, then copy in the new
        // values.

        int keyOffset = key.getOffset();
        int entryOffset = keyOffset - 4;
        dbPage.moveDataRange(entryOffset, entryOffset + len, len);
        
        dbPage.writeShort(entryOffset, newFilePtr.getPageNo());
        dbPage.writeShort(entryOffset, newFilePtr.getOffset());

        PageTuple.storeTuple(dbPage, keyOffset, colInfos, newKey);

        // Increment the total number of entries.
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries + 1);

        // Reload the page contents now that we have a new key.
        // TODO:  We could do this more efficiently, but this should be
        //        sufficient for now.
        loadPageContents();
        
        return new IndexPointer(newFilePtr.getPageNo(), newFilePtr.getOffset());
    }
}
