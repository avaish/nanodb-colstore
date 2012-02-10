package edu.caltech.nanodb.storage.btreeindex;


import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import org.apache.log4j.Logger;


/**
 */
public class NonLeafPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(NonLeafPage.class);


    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;


    /** The offset where the parent page number is stored in this page. */
    public static final int OFFSET_PARENT_PAGE_NO = 1;

    /**
     * The offset where the previous-sibling page number is stored in this page.
     * The only leaf page that doesn't have a previous sibling is the first leaf
     * in the index.
     */
    public static final int OFFSET_PREV_PAGE_NO = 3;


    /**
     * The offset where the next-sibling page number is stored in this page.
     * The only leaf page that doesn't have a next sibling is the last leaf
     * in the index.
     */
    public static final int OFFSET_NEXT_PAGE_NO = 5;


    /**
     * The offset where the number of pointer entries is stored in the page.
     * The page will hold one less keys than pointers, since each key must be
     * sandwiched between two pointers.
     */
    public static final int OFFSET_NUM_POINTERS = 7;


    /** The offset of the first pointer in the non-leaf page. */
    public static final int OFFSET_FIRST_POINTER = 9;


    private DBPage dbPage;


    private IndexFileInfo idxFileInfo;


    /** The number of pointers stored within this non-leaf page. */
    private int numPointers;


    /**
     * An array of the offsets where the pointers are stored in this non-leaf
     * page.  Each pointer points to another page within the index file.  There
     * is one more pointer than keys, since each key must be sandwiched between
     * two pointers.
     */
    private int[] pointerOffsets;


    /**
     * An array of the keys stored in this non-leaf page.  Each key also stores
     * the file-pointer for the associated tuple, as the last value in the key.
     */
    private BTreeIndexPageTuple[] keys;


    /**
     * The total size of all data (pointers + keys + initial values) stored
     * within this leaf page.  This is also the offset at which we can start
     * writing more data without overwriting anything.
     */
    private int endOffset;

    
    public NonLeafPage(DBPage dbPage, IndexFileInfo idxFileInfo) {
        this.dbPage = dbPage;
        this.idxFileInfo = idxFileInfo;

        loadPageContents();
    }


    /**
     * This static helper function initializes a leaf index-page with the type
     * and detail values that will allow a new {@code LeafPage} wrapper to be
     * initialized for the page, and then returns a newly initialized wrapper
     * object.
     *
     * @param dbPage the page to initialize as a leaf-page.
     *
     * @param idxFileInfo details about the index that the leaf-page is for
     *
     * @return a newly initialized {@code LeafPage} object wrapping the page
     */
    public static NonLeafPage init(DBPage dbPage, IndexFileInfo idxFileInfo,
                                   int ptr0, Tuple key0, int ptr1) {

        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_INNER_PAGE);

        dbPage.writeShort(OFFSET_PARENT_PAGE_NO, 0);
        dbPage.writeShort(OFFSET_PREV_PAGE_NO, 0);
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, 0);

        // Write the first contents of the non-leaf page:  [ptr0, key0, ptr1]
        // Since key0 will usually be a BTreeIndexPageTuple, we have to rely on
        // the storeTuple() method to tell us where the new tuple's data ends.

        int offset = OFFSET_FIRST_POINTER;

        dbPage.writeShort(offset, ptr0);
        offset += 2;

        offset = PageTuple.storeTuple(dbPage, offset,
            idxFileInfo.getIndexSchema(), key0);

        dbPage.writeShort(offset, ptr1);

        dbPage.writeShort(OFFSET_NUM_POINTERS, 2);

        return new NonLeafPage(dbPage, idxFileInfo);
    }


    private void loadPageContents() {
        numPointers = dbPage.readUnsignedShort(OFFSET_NUM_POINTERS);
        if (numPointers > 0) {
            pointerOffsets = new int[numPointers];
            keys = new BTreeIndexPageTuple[numPointers - 1];

            List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();

            // Handle first pointer + key separately since we know their offsets

            pointerOffsets[0] = OFFSET_FIRST_POINTER;
            BTreeIndexPageTuple key = new BTreeIndexPageTuple(dbPage,
                OFFSET_FIRST_POINTER + 2, colInfos);
            keys[0] = key;

            // Handle all the pointer/key pairs.  This excludes the last
            // pointer.

            int keyEndOffset;
            for (int i = 1; i < numPointers - 1; i++) {
                // Next pointer starts where the previous key ends.
                keyEndOffset = key.getEndOffset();
                pointerOffsets[i] = keyEndOffset;
                
                // Next key starts after the next pointer.
                key = new BTreeIndexPageTuple(dbPage, keyEndOffset + 2, colInfos);
                keys[i] = key;
            }

            keyEndOffset = key.getEndOffset();
            pointerOffsets[numPointers - 1] = keyEndOffset;
            endOffset = keyEndOffset + 2;
        }
        else {
            // There are no entries (pointers + keys).
            endOffset = OFFSET_FIRST_POINTER;
            pointerOffsets = null;
            keys = null;
        }
    }


    public IndexFileInfo getIndexFileInfo() {
        return idxFileInfo;
    }


    public int getPageNo() {
        return dbPage.getPageNo();
    }


    public int getParentPageNo() {
        return dbPage.readUnsignedShort(OFFSET_PARENT_PAGE_NO);
    }


    public void setParentPageNo(int pageNo) {
        dbPage.writeShort(OFFSET_PARENT_PAGE_NO, pageNo);
    }


    public int getPrevPageNo() {
        return dbPage.readUnsignedShort(OFFSET_PREV_PAGE_NO);
    }


    public void setPrevPageNo(int pageNo) {
        dbPage.writeShort(OFFSET_PREV_PAGE_NO, pageNo);
    }


    public int getNextPageNo() {
        return dbPage.readUnsignedShort(OFFSET_NEXT_PAGE_NO);
    }


    public void setNextPageNo(int pageNo) {
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, pageNo);
    }


    public int getNumPointers() {
        return numPointers;
    }
    
    
    public int getNumKeys() {
        if (numPointers < 1) {
            throw new IllegalStateException("Non-leaf page contains no " +
                "pointers.  Number of keys is meaningless.");
        }
        
        return numPointers - 1;
    }


    public int getFreeSpace() {
        return dbPage.getPageSize() - endOffset;
    }

    
    public int getPointer(int index) {
        return dbPage.readUnsignedShort(pointerOffsets[index]);
    }
    
    
    public int getIndexOfPointer(int pointer) {
        for (int i = 0; i < getNumPointers(); i++) {
            if (getPointer(i) == pointer)
                return i;
        }

        return -1;
    }
    

    public BTreeIndexPageTuple getKey(int index) {
        return keys[index];
    }

    
    public void replaceKey(int index, Tuple key) {
        int oldStart = keys[index].getOffset();
        int oldLen = keys[index].getEndOffset() - oldStart;
        
        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int newLen = PageTuple.getTupleStorageSize(colInfos, key);
        
        if (newLen != oldLen) {
            // Need to adjust the amount of space the key takes.
            
            if (endOffset + newLen - oldLen > dbPage.getPageSize()) {
                throw new IllegalArgumentException(
                    "New key-value is too large to fit in non-leaf page.");
            }

            dbPage.moveDataRange(oldStart + oldLen, oldStart + newLen,
                endOffset - oldStart - oldLen);
        }

        PageTuple.storeTuple(dbPage, oldStart, colInfos, key);

        // Reload the page contents.
        // TODO:  This is slow, but it should be fine for now.
        loadPageContents();
    }
    
    
    public void addEntry(int pageNo1, Tuple key, int pageNo2) {

        if (logger.isDebugEnabled()) {
            logger.debug("Non-leaf page " + getPageNo() +
                " contents before adding entry:");
            for (int p = 0; p < numPointers; p++)
                logger.debug("    Index " + p + " = page " + getPointer(p));
        }

        int i;
        for (i = 0; i < numPointers; i++) {
            if (getPointer(i) == pageNo1)
                break;
        }
        
        logger.debug(String.format("Found page-pointer %d in index %d",
            pageNo1, i));

        if (i == numPointers) {
            throw new IllegalArgumentException(
                "Can't find initial page-pointer " + pageNo1 +
                " in non-leaf page " + getPageNo());
        }
        
        // Figure out where to insert the new key and value.

        int oldKeyStart;
        if (i < numPointers - 1) {
            // There's a key i associated with pointer i.  Use the key's offset,
            // since it's after the pointer.
            oldKeyStart = keys[i].getOffset();
        }
        else {
            // The pageNo1 pointer is the last pointer in the sequence.  Use
            // the end-offset of the data in the page.
            oldKeyStart = endOffset;
        }
        int len = endOffset - oldKeyStart;

        // Compute the size of the new key and pointer, and make sure they fit
        // into the page.

        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int newKeySize = PageTuple.getTupleStorageSize(colInfos, key);
        int newEntrySize = newKeySize + 2;
        if (endOffset + newEntrySize > dbPage.getPageSize()) {
            throw new IllegalArgumentException("New key-value and " +
                "page-pointer are too large to fit in non-leaf page.");
        }

        if (len > 0) {
            // Move the data after the pageNo1 pointer to make room for
            // the new key and pointer.
            dbPage.moveDataRange(oldKeyStart, oldKeyStart + newEntrySize,
                newEntrySize);
        }

        // Write in the new key/pointer values.
        PageTuple.storeTuple(dbPage, oldKeyStart, colInfos, key);
        dbPage.writeShort(oldKeyStart + newKeySize, pageNo2);

        if (logger.isDebugEnabled()) {
            logger.debug("Non-leaf page " + getPageNo() +
                " contents after adding entry:");
            for (int p = 0; p < numPointers; p++)
                logger.debug("    Index " + p + " = page " + getPointer(p));
        }

        // Finally, increment the number of pointers in the page, then reload
        // the cached data.

        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers + 1);

        loadPageContents();
    }
    

/*
    public int getKeySize(int index) {
        BTreeIndexPageTuple key = getKey(index);
        return key.getEndOffset() - key.getOffset();
    }
*/
}
