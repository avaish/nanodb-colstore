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
     * The offset where the number of pointer entries is stored in the page.
     * The page will hold one less keys than pointers, since each key must be
     * sandwiched between two pointers.
     */
    public static final int OFFSET_NUM_POINTERS = 3;


    /** The offset of the first pointer in the non-leaf page. */
    public static final int OFFSET_FIRST_POINTER = 5;


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


    public static NonLeafPage init(DBPage dbPage, IndexFileInfo idxFileInfo) {

        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_INNER_PAGE);

        dbPage.writeShort(OFFSET_PARENT_PAGE_NO, 0);
        dbPage.writeShort(OFFSET_NUM_POINTERS, 0);

        return new NonLeafPage(dbPage, idxFileInfo);
    }



    public static NonLeafPage init(DBPage dbPage, IndexFileInfo idxFileInfo,
                                   int ptr0, Tuple key0, int ptr1) {

        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_INNER_PAGE);

        dbPage.writeShort(OFFSET_PARENT_PAGE_NO, 0);

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
            dbPage.moveDataRange(oldKeyStart, oldKeyStart + newEntrySize, len);
        }

        // Write in the new key/pointer values.
        PageTuple.storeTuple(dbPage, oldKeyStart, colInfos, key);
        dbPage.writeShort(oldKeyStart + newKeySize, pageNo2);

        // Finally, increment the number of pointers in the page, then reload
        // the cached data.

        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers + 1);

        loadPageContents();

        if (logger.isDebugEnabled()) {
            logger.debug("Non-leaf page " + getPageNo() +
                " contents after adding entry:");
            for (int p = 0; p < numPointers; p++)
                logger.debug("    Index " + p + " = page " + getPointer(p));
        }
    }


    public TupleLiteral movePointersLeft(NonLeafPage leftSibling, int count,
                                         Tuple parentKey) {

        if (leftSibling.getParentPageNo() != getParentPageNo()) {
            throw new IllegalArgumentException(
                "leftSibling doesn't have the same parent as this node");
        }

        if (count < 0 || count > numPointers) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numPointers + "), got " + count);
        }

        int moveEndOffset = pointerOffsets[count] + 2;
        int len = moveEndOffset - OFFSET_FIRST_POINTER;

        // The parent-key can be null if we are splitting a page into two pages.
        // However, this situation is only valid if the right sibling is EMPTY.
        int parentKeyLen = 0;
        if (parentKey != null) {
            parentKeyLen = PageTuple.getTupleStorageSize(
                idxFileInfo.getIndexSchema(), parentKey);
        }
        else {
            if (leftSibling.getNumPointers() != 0) {
                throw new IllegalStateException("Cannot move pointers to " +
                    "non-empty sibling if no parent-key is specified!");
            }
        }
        
        // Copy the range of pointer-data to the destination page, making sure
        // to include the parent-key before the first pointer from the right
        // page.  Then update the count of pointers in the destination page.

        if (parentKey != null) {
            // Write in the parent key
            PageTuple.storeTuple(leftSibling.dbPage, leftSibling.endOffset,
                idxFileInfo.getIndexSchema(), parentKey);
        }

        // Copy the pointer data across
        leftSibling.dbPage.write(leftSibling.endOffset + parentKeyLen,
            dbPage.getPageData(), OFFSET_FIRST_POINTER, len);

        if (parentKey != null) {
            // Update the entry-count
            leftSibling.dbPage.writeShort(OFFSET_NUM_POINTERS,
                leftSibling.numPointers + count);
        }

        // Finally, pull out the new parent key, and then clear the area in the
        // source page that no longer holds data.

        TupleLiteral newParentKey = null;
        if (count < numPointers) {
            // There's a key to the right of the last pointer we moved.  This
            // will become the new parent key.
            BTreeIndexPageTuple key = keys[count - 1];
            int keyEndOff = key.getEndOffset();
            newParentKey = new TupleLiteral(key);

            // Slide left the remainder of the data.
            dbPage.moveDataRange(keyEndOff, OFFSET_FIRST_POINTER,
                endOffset - keyEndOff);
            dbPage.setDataRange(OFFSET_FIRST_POINTER + endOffset - keyEndOff,
                keyEndOff - OFFSET_FIRST_POINTER, (byte) 0);
        }
        else {
            // The entire page is being emptied, so clear out all the data.
            dbPage.setDataRange(OFFSET_FIRST_POINTER,
                endOffset - OFFSET_FIRST_POINTER, (byte) 0);
        }
        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers - count);

        // Update the cached info for both non-leaf pages.
        loadPageContents();
        leftSibling.loadPageContents();

        return newParentKey;
    }


    public TupleLiteral movePointersRight(NonLeafPage rightSibling, int count,
                                          Tuple parentKey) {

        if (rightSibling.getParentPageNo() != getParentPageNo()) {
            throw new IllegalArgumentException(
                "rightSibling doesn't have the same parent as this node");
        }

        if (count < 0 || count > numPointers) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numPointers + "), got " + count);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Non-leaf page " + getPageNo() +
                " contents before moving pointers right:");
            logger.debug(toFormattedString());
        }

        int startPointerIndex = numPointers - count;
        int startOffset = pointerOffsets[startPointerIndex];
        int len = endOffset - startOffset;
        
        logger.debug("Moving everything after pointer " + startPointerIndex +
            " to right sibling.  Start offset = " + startOffset +
            ", end offset = " + endOffset + ", len = " + len);

        // The parent-key can be null if we are splitting a page into two pages.
        // However, this situation is only valid if the right sibling is EMPTY.
        int parentKeyLen = 0;
        if (parentKey != null) {
            parentKeyLen = PageTuple.getTupleStorageSize(
                idxFileInfo.getIndexSchema(), parentKey);
        }
        else {
            if (rightSibling.getNumPointers() != 0) {
                throw new IllegalStateException("Cannot move pointers to " +
                    "non-empty sibling if no parent-key is specified!");
            }
        }

        // Copy the range of pointer-data to the destination page, making sure
        // to include the parent-key after the last pointer from the left page.
        // Then update the count of pointers in the destination page.

        if (parentKey != null) {
            // Make room for the data
            rightSibling.dbPage.moveDataRange(OFFSET_FIRST_POINTER,
                OFFSET_FIRST_POINTER + len + parentKeyLen,
                rightSibling.endOffset - OFFSET_FIRST_POINTER);
        }

        // Copy the pointer data across
        rightSibling.dbPage.write(OFFSET_FIRST_POINTER, dbPage.getPageData(),
            startOffset, len);

        if (parentKey != null) {
            // Write in the parent key
            PageTuple.storeTuple(rightSibling.dbPage, OFFSET_FIRST_POINTER + len,
                idxFileInfo.getIndexSchema(), parentKey);
        }

        // Update the entry-count
        rightSibling.dbPage.writeShort(OFFSET_NUM_POINTERS,
            rightSibling.numPointers + count);

        // Finally, pull out the new parent key, and then clear the area in the
        // source page that no longer holds data.

        TupleLiteral newParentKey = null;
        if (count < numPointers) {
            // There's a key to the left of the last pointer we moved.  This
            // will become the new parent key.
            BTreeIndexPageTuple key = keys[startPointerIndex - 1];
            int keyOff = key.getOffset();
            newParentKey = new TupleLiteral(key);
            
            // Cut down the remainder of the data.
            dbPage.setDataRange(keyOff, endOffset - keyOff, (byte) 0);
        }
        else {
            // The entire page is being emptied, so clear out all the data.
            dbPage.setDataRange(OFFSET_FIRST_POINTER,
                endOffset - OFFSET_FIRST_POINTER, (byte) 0);
        }
        dbPage.writeShort(OFFSET_NUM_POINTERS, numPointers - count);

        // Update the cached info for both non-leaf pages.
        loadPageContents();
        rightSibling.loadPageContents();

        if (logger.isDebugEnabled()) {
            logger.debug("Non-leaf page " + getPageNo() +
                " contents after moving pointers right:");
            logger.debug(toFormattedString());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Right-sibling page " + rightSibling.getPageNo() +
                " contents after moving pointers right:");
            logger.debug(rightSibling.toFormattedString());
        }

        return newParentKey;
    }
    
    
    public String toFormattedString() {
        StringBuilder buf = new StringBuilder();

        buf.append(String.format("Inner page %d contains %d pointers%n",
            getPageNo(), numPointers));

        if (numPointers > 0) {
            for (int i = 0; i < numPointers - 1; i++) {
                buf.append(String.format("    Pointer %d = page %d%n", i,
                    getPointer(i)));
                buf.append(String.format("    Key %d = %s%n", i, getKey(i)));
            }
            buf.append(String.format("    Pointer %d = page %d%n", numPointers - 1,
                getPointer(numPointers - 1)));
        }

        return buf.toString();
    }
}
