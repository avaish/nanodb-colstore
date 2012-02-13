package edu.caltech.nanodb.storage.btreeindex;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;


/**
 *
 */
public class LeafPage {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(LeafPage.class);


    /** The page type always occupies the first byte of the page. */
    public static final int OFFSET_PAGE_TYPE = 0;


    /** The offset where the parent page number is stored in this page. */
    public static final int OFFSET_PARENT_PAGE_NO = 1;


    /**
     * The offset where the next-sibling page number is stored in this page.
     * The only leaf page that doesn't have a next sibling is the last leaf
     * in the index; its "next page" value will be set to 0.
     */
    public static final int OFFSET_NEXT_PAGE_NO = 3;


    /**
     * The offset where the number of key+pointer entries is stored in the page.
     */
    public static final int OFFSET_NUM_ENTRIES = 5;


    /** The offset of the first key in the leaf page. */
    public static final int OFFSET_FIRST_KEY = 7;

    
    private DBPage dbPage;

    
    private IndexFileInfo idxFileInfo;
    

    private List<ColumnInfo> colInfos;
    
    
    /** The number of entries (pointers + keys) stored within this leaf page. */
    private int numEntries;


    /**
     * A list of the keys stored in this leaf page.  Each key also stores the
     * file-pointer for the associated tuple, as the last value in the key.
     */
    private ArrayList<BTreeIndexPageTuple> keys;


    /**
     * The total size of all data (pointers + keys + initial values) stored
     * within this leaf page.  This is also the offset at which we can start
     * writing more data without overwriting anything.
     */
    private int endOffset;


    public LeafPage(DBPage dbPage, IndexFileInfo idxFileInfo) {
        this.dbPage = dbPage;
        this.idxFileInfo = idxFileInfo;
        this.colInfos = idxFileInfo.getIndexSchema();

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
    public static LeafPage init(DBPage dbPage, IndexFileInfo idxFileInfo) {
        dbPage.writeByte(OFFSET_PAGE_TYPE, BTreeIndexManager.BTREE_LEAF_PAGE);

        dbPage.writeShort(OFFSET_NUM_ENTRIES, 0);

        dbPage.writeShort(OFFSET_PARENT_PAGE_NO, 0);
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, 0);

        return new LeafPage(dbPage, idxFileInfo);
    }


    private void loadPageContents() {
        numEntries = dbPage.readUnsignedShort(OFFSET_NUM_ENTRIES);
        keys = new ArrayList<BTreeIndexPageTuple>(numEntries);

        if (numEntries > 0) {
            // Handle first key separately since we know its offset.

            BTreeIndexPageTuple key =
                new BTreeIndexPageTuple(dbPage, OFFSET_FIRST_KEY, colInfos);

            keys.add(key);

            // Handle remaining keys.
            for (int i = 1; i < numEntries; i++) {
                int keyEndOffset = key.getEndOffset();
                key = new BTreeIndexPageTuple(dbPage, keyEndOffset, colInfos);
                keys.add(key);
            }

            endOffset = key.getEndOffset();
        }
        else {
            // There are no entries (pointers + keys).
            endOffset = OFFSET_FIRST_KEY;
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

    
    public int getNextPageNo() {
        return dbPage.readUnsignedShort(OFFSET_NEXT_PAGE_NO);
    }


    public void setNextPageNo(int pageNo) {
        dbPage.writeShort(OFFSET_NEXT_PAGE_NO, pageNo);
    }


    public int getNumEntries() {
        return numEntries;
    }


    public int getFreeSpace() {
        return dbPage.getPageSize() - endOffset;
    }

    
    public BTreeIndexPageTuple getKey(int index) {
        return keys.get(index);
    }
    
    
    public int getKeySize(int index) {
        BTreeIndexPageTuple key = getKey(index);
        return key.getEndOffset() - key.getOffset();
    }


    public void addEntry(TupleLiteral newKey) {
        if (newKey.getStorageSize() == -1) {
            throw new IllegalArgumentException("New key's storage size must " +
                "be computed before this method is called.");
        }

        if (getFreeSpace() < newKey.getStorageSize()) {
            throw new IllegalArgumentException(String.format(
                "Not enough space in this node to store the new key " +
                "(%d bytes free; %d bytes required)", getFreeSpace(),
                newKey.getStorageSize()));
        }

        if (numEntries == 0) {
            logger.debug("Leaf page is empty; storing new entry at start.");
            addEntryAtIndex(newKey, 0);
        }
        else {
            int i;
            for (i = 0; i < numEntries; i++) {
                BTreeIndexPageTuple key = keys.get(i);

                /* This gets REALLY verbose...
                logger.debug(i + ":  comparing " + newKey + " to " + key);
                */

                // Compare the tuple to the current key.  Once we find where the
                // new key/tuple should go, copy the key/pointer into the page.
                if (TupleComparator.compareTuples(newKey, key) < 0) {
                    logger.debug("Storing new entry at index " + i +
                        " in the leaf page.");
                    addEntryAtIndex(newKey, i);
                    break;
                }
            }

            if (i == numEntries) {
                // The new tuple will go at the end of this page's entries.
                logger.debug("Storing new entry at end of leaf page.");
                addEntryAtIndex(newKey, numEntries);
            }
        }

        // The addEntryAtIndex() method updates the internal fields that cache
        // where keys live, etc.
    }


    private void addEntryAtIndex(TupleLiteral newKey, int index) {
        logger.debug("Leaf-page is starting with data ending at index " +
            endOffset + ", and has " + numEntries + " entries.");

        // Get the length of the new tuple, and add in the size of the
        // file-pointer as well.
        int len = newKey.getStorageSize();
        if (len == -1) {
            throw new IllegalArgumentException("New key's storage size must " +
                "be computed before this method is called.");
        }

        logger.debug("New key's storage size is " + len + " bytes");

        int keyOffset;
        if (index < numEntries) {
            // Need to slide keys after this index over, in order to make space.

            BTreeIndexPageTuple key = getKey(index);

            // Make space for the new key/pointer to be stored, then copy in
            // the new values.

            keyOffset = key.getOffset();

            logger.debug("Moving leaf-page data in range [" + keyOffset + ", " +
                endOffset + ") over by " + len + " bytes");

            dbPage.moveDataRange(keyOffset, keyOffset + len, endOffset - keyOffset);
        }
        else {
            // The new key falls at the end of the data in the leaf index page.
            keyOffset = endOffset;
            logger.debug("New key is at end of leaf-page data; not moving anything.");
        }

        // Write the key and its associated file-pointer value into the page.
        PageTuple.storeTuple(dbPage, keyOffset, colInfos, newKey);

        // Increment the total number of entries.
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries + 1);

        // Reload the page contents now that we have a new key in the mix.
        // TODO:  We could do this more efficiently, but this should be
        //        sufficient for now.
        loadPageContents();

        logger.debug("Wrote new key to leaf-page at offset " + keyOffset + ".");
        logger.debug("Leaf-page is ending with data ending at index " +
            endOffset + ", and has " + numEntries + " entries.");
    }


    public void moveEntriesLeft(LeafPage leftSibling, int count) {
        if (leftSibling == null)
            throw new IllegalArgumentException("leftSibling cannot be null");

        if (leftSibling.getParentPageNo() != getParentPageNo()) {
            throw new IllegalArgumentException("leftSibling doesn't have the" +
                " same parent as this node");
        }

        if (leftSibling.getNextPageNo() != getPageNo()) {
            throw new IllegalArgumentException("leftSibling " +
                leftSibling.getPageNo() + " isn't actually the left " +
                "sibling of this leaf-node " + getPageNo());
        }

        if (count < 0 || count > numEntries) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numEntries + "), got " + count);
        }

        int moveEndOffset = getKey(count).getOffset();
        int len = moveEndOffset - OFFSET_FIRST_KEY;

        // Copy the range of key-data to the destination page.  Then update the
        // count of entries in the destination page.
        // Don't need to move any data in the left sibling; we are appending!
        leftSibling.dbPage.write(leftSibling.endOffset, dbPage.getPageData(),
            OFFSET_FIRST_KEY, len);             // Copy the key-data across
        leftSibling.dbPage.writeShort(OFFSET_NUM_ENTRIES,
            leftSibling.numEntries + count);    // Update the entry-count

        // Remove that range of key-data from this page.
        dbPage.moveDataRange(moveEndOffset, OFFSET_FIRST_KEY,
            endOffset - moveEndOffset);
        dbPage.setDataRange(endOffset - len, len, (byte) 0);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries - count);

        // Update the cached info for both leaves.
        loadPageContents();
        leftSibling.loadPageContents();
    }


    
    public void moveEntriesRight(LeafPage rightSibling, int count) {
        if (rightSibling == null)
            throw new IllegalArgumentException("rightSibling cannot be null");

        if (rightSibling.getParentPageNo() != getParentPageNo()) {
            throw new IllegalArgumentException("rightSibling doesn't have the" +
                " same parent as this node");
        }

        if (getNextPageNo() != rightSibling.getPageNo()) {
            throw new IllegalArgumentException("rightSibling " +
                rightSibling.getPageNo() + " isn't actually the right " +
                "sibling of this leaf-node " + getPageNo());
        }
        
        if (count < 0 || count > numEntries) {
            throw new IllegalArgumentException("count must be in range [0, " +
                numEntries + "), got " + count);
        }

        int startOffset = getKey(numEntries - count).getOffset();
        int len = endOffset - startOffset;

        // Copy the range of key-data to the destination page.  Then update the
        // count of entries in the destination page.

        // Make room for the data
        rightSibling.dbPage.moveDataRange(OFFSET_FIRST_KEY,
            OFFSET_FIRST_KEY + len, rightSibling.endOffset - OFFSET_FIRST_KEY);

        // Copy the key-data across
        rightSibling.dbPage.write(OFFSET_FIRST_KEY, dbPage.getPageData(),
            startOffset, len);

        // Update the entry-count
        rightSibling.dbPage.writeShort(OFFSET_NUM_ENTRIES,
            rightSibling.numEntries + count);

        // Remove that range of key-data from this page.
        dbPage.setDataRange(startOffset, len, (byte) 0);
        dbPage.writeShort(OFFSET_NUM_ENTRIES, numEntries - count);

        // Update the cached info for both leaves.
        loadPageContents();
        rightSibling.loadPageContents();
    }

}
