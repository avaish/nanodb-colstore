package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;

import java.util.List;

import edu.caltech.nanodb.expressions.TupleComparator;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.indexes.IndexManager;

import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This is the class that manages B<sup>+</sup> tree indexes.  These indexes are
 * used for enforcing primary and candidate keys, and also providing optimized
 * access to tuples with specific values.
 * </p>
 * <p>
 * B<sup>+</sup> tree indexes are comprised of three kinds of pages:
 * </p>
 * <ul>
 * <li>Page 0 is always a header page, and specifies the entry-points in the
 *     hierarchy:  the root page of the tree, and the first and last leaves of
 *     the tree.  Page 0 also maintains a list of empty pages in the tree, so
 *     that adding new nodes to the tree is fast.  (See the {@link HeaderPage}
 *     class for details.)</li>
 * <li>The remaining (non-free) pages are either leaf nodes or non-leaf nodes.
 *     The first byte of the page indicates whether it is a leaf page or a
 *     non-leaf page, the next short value specifies how many entries are in the
 *     page, and the remaining space is used for the indexing structure.  (See
 *     the {@link NonLeafPage} and {@link LeafPage} classes for details.)
 * </li>
 * </ul>
 */
public class BTreeIndexManager implements IndexManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BTreeIndexManager.class);


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the page
     * is an inner (i.e. non-leaf) page.
     */
    public static final int BTREE_INNER_PAGE = 1;


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the page
     * is a leaf page.
     */
    public static final int BTREE_LEAF_PAGE = 2;


    /**
     * This value is stored in a B-tree page's byte 0, to indicate that the page
     * is empty.
     */
    public static final int BTREE_EMPTY_PAGE = 2;


    /**
     * The table manager uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;


    /**
     * Initializes the heap-file table manager.  This class shouldn't be
     * initialized directly, since the storage manager will initialize it when
     * necessary.
     *
     * @param storageManager the storage manager that is using this table manager
     *
     * @throws IllegalArgumentException if <tt>storageManager</tt> is <tt>null</tt>
     */
    public BTreeIndexManager(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }


    /**
     * This helper function generates the prefix of a name for an index with no
     * actual name specified.  Since indexes and other constraints don't
     * necessarily require names to be specified, we need some way to generate
     * these names.
     *
     * @param idxFileInfo the information describing the index to be named
     *
     * @return a string containing a prefix to use for naming the index.
     */
    public String getUnnamedIndexPrefix(IndexFileInfo idxFileInfo) {
        // Generate a prefix based on the contents of the IndexFileInfo object.
        IndexInfo info = idxFileInfo.getIndexInfo();
        TableConstraintType constraintType = info.getConstraintType();

        if (constraintType == null)
            return "IDX_" + idxFileInfo.getTableName();

        switch (info.getConstraintType()) {
            case PRIMARY_KEY:
                return "PK_" + idxFileInfo.getTableName();

            case UNIQUE:
                return "CK_" + idxFileInfo.getTableName();

            default:
                throw new IllegalArgumentException("Unrecognized constraint type " +
                    constraintType);
        }
    }


    // Copy interface javadocs.
    @Override
    public void initIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        String indexName = idxFileInfo.getIndexName();
        String tableName = idxFileInfo.getTableName();
        DBFile dbFile = idxFileInfo.getDBFile();

        //Schema schema = idxFileInfo.getSchema();

        logger.info(String.format(
            "Initializing new index %s on table %s, stored at %s", indexName,
            tableName, dbFile));

        // The index's header page just stores details of the indexing structure
        // itself, since the the actual schema information and other index
        // details are stored in the referenced table.

        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        HeaderPage.setRootPageNo(headerPage, 0);
        HeaderPage.setFirstLeafPageNo(headerPage, 0);
        HeaderPage.setLastLeafPageNo(headerPage, 0);
        HeaderPage.setFirstEmptyPageNo(headerPage, 0);
    }


    /**
     * This method reads in the schema and other critical information for the
     * specified table.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         table's schema and other details.
     */
    public void loadIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        // For now, we don't need to do anything in this method.
    }


    @Override
    public void addTuple(IndexFileInfo idxFileInfo, PageTuple tup)
        throws IOException {

        // These are the values we store into the index for the tuple:  the key,
        // and a file-pointer to the tuple that the key is for.
        TupleLiteral newTupleKey = makeStoredKeyValue(idxFileInfo, tup);

        logger.debug("Adding search-key value " + newTupleKey + " to index " +
            idxFileInfo.getIndexName());

        // Navigate to the leaf-page, creating one if the index is currently
        // empty.
        LeafPage leaf = navigateToLeafPage(idxFileInfo, newTupleKey, true);
        addEntryToLeafPage(leaf, newTupleKey);
    }
    
    
    private void addEntryToLeafPage(LeafPage leaf, TupleLiteral newTupleKey)
        throws IOException {

        // Figure out where the new key-value goes in the leaf page.

        int newEntrySize = newTupleKey.getStorageSize();
        if (leaf.getFreeSpace() < newEntrySize) {
            // Try to relocate entries from this leaf to either sibling,
            // or if that can't happen, split the leaf page into two.
            if (!relocateLeafEntriesAndAddKey(leaf, newTupleKey))
                splitLeafAndAddKey(leaf, newTupleKey);
        }
        else {
            // There is room in the leaf for the new key.  Add it there.
            leaf.addEntry(newTupleKey);
        }
    }

    
    private LeafPage loadLeafPage(IndexFileInfo idxFileInfo, int pageNo)
        throws IOException {

        if (pageNo == 0)
            return null;

        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new LeafPage(dbPage, idxFileInfo);
    }


    private NonLeafPage loadNonLeafPage(IndexFileInfo idxFileInfo, int pageNo)
        throws IOException {

        if (pageNo == 0)
            return null;

        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new NonLeafPage(dbPage, idxFileInfo);
    }


    private LeafPage navigateToLeafPage(IndexFileInfo idxFileInfo,
        TupleLiteral searchKey, boolean createIfNeeded) throws IOException {

        String indexName = idxFileInfo.getIndexName();

        // The header page tells us where the root page starts.
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Get the root page of the index.
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        DBPage dbpRoot;
        if (rootPageNo == 0) {
            // The index doesn't have any data-pages at all yet.  Create one if
            // the caller wants it.

            if (!createIfNeeded)
                return null;

            // We need to create a brand new leaf page and make it the root.

            logger.debug("Index " + indexName + " currently has no data " +
                "pages; finding/creating one to use as the root!");

            dbpRoot = getNewDataPage(dbFile);
            rootPageNo = dbpRoot.getPageNo();

            HeaderPage.setRootPageNo(dbpHeader, rootPageNo);
            HeaderPage.setFirstLeafPageNo(dbpHeader, rootPageNo);
            HeaderPage.setLastLeafPageNo(dbpHeader, rootPageNo);

            dbpRoot.writeByte(0, BTREE_LEAF_PAGE);
            LeafPage.init(dbpRoot, idxFileInfo);

            logger.debug("New root pageNo is " + rootPageNo);
        }
        else {
            // The index has a root page; load it.
            dbpRoot = storageManager.loadDBPage(dbFile, rootPageNo);

            logger.debug("Index " + idxFileInfo.getIndexName() +
                " root pageNo is " + rootPageNo);
        }

        // Next, descend down the index's structure until we find the proper
        // leaf-page based on the key value(s).

        DBPage dbPage = dbpRoot;
        int pageType = dbPage.readByte(0);
        if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
            throw new IOException("Invalid page type encountered:  " + pageType);

        while (pageType != BTREE_LEAF_PAGE) {
            logger.debug("Examining non-leaf page " + dbPage.getPageNo() +
                " of index " + indexName);

            int nextPageNo = -1;
            
            NonLeafPage nonLeafPage = new NonLeafPage(dbPage, idxFileInfo);

            int numKeys = nonLeafPage.getNumKeys();
            if (numKeys < 1) {
                throw new IllegalStateException("Non-leaf page " +
                    dbPage.getPageNo() + " is invalid:  it contains no keys!");
            }

            for (int i = 0; i < numKeys; i++) {
                BTreeIndexPageTuple key = nonLeafPage.getKey(i);
                int cmp = TupleComparator.compareTuples(searchKey, key);
                if (cmp < 0) {
                    logger.debug("Value is less than key at index " + i +
                        "; following pointer " + i + " before this key.");
                    nextPageNo = nonLeafPage.getPointer(i);
                    break;
                }
                else if (cmp == 0) {
                    logger.debug("Value is equal to key at index " + i +
                        "; following pointer " + (i+1) + " after this key.");
                    nextPageNo = nonLeafPage.getPointer(i + 1);
                    break;
                }
            }

            if (nextPageNo == -1) {
                // None of the other entries in the page matched the tuple.  Get
                // the last pointer in the page.
                logger.debug("Value is greater than all keys in this page;" +
                    " following last pointer " + numKeys + " in the page.");
                nextPageNo = nonLeafPage.getPointer(numKeys);
            }

            // Navigate to the next page in the index.
            dbPage = storageManager.loadDBPage(dbFile, nextPageNo);
            pageType = dbPage.readByte(0);
            if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
                throw new IOException("Invalid page type encountered:  " + pageType);
        }

        return new LeafPage(dbPage, idxFileInfo);
    }


    /**
     * This helper function finds and returns a new data page, either by taking
     * it from the empty-pages list in the index file, or if this list is empty,
     * creating a brand new page at the end of the file.
     *
     * @param dbFile the index file to get a new empty data page from
     *
     * @return an empty {@code DBPage} that can be used as a new index page.
     *
     * @throws IOException if an error occurs while loading a data page, or
     *         while extending the size of the index file.
     */
    private DBPage getNewDataPage(DBFile dbFile) throws IOException {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        DBPage newPage;
        int pageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);

        logger.debug("First empty page number is " + pageNo);

        if (pageNo == 0) {
            // There are no empty pages.  Create a new page to use.
            int numPages = dbFile.getNumPages();
            newPage = storageManager.loadDBPage(dbFile, numPages, true);
        }
        else {
            // Load the empty page, and remove it from the chain of empty pages.
            newPage = storageManager.loadDBPage(dbFile, pageNo);
            int nextEmptyPage = newPage.readUnsignedShort(1);
            HeaderPage.setFirstEmptyPageNo(dbpHeader, nextEmptyPage);
        }

        logger.debug("Found new data page for the index:  page " +
            newPage.getPageNo());

        // TODO:  Increment the number of data pages?

        return newPage;
    }

    
    private boolean relocateLeafEntriesAndAddKey(LeafPage page,
        TupleLiteral key) throws IOException {

        // See if we are able to relocate records either direction to free up
        // space for the new key.

        int bytesRequired = key.getStorageSize();

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();

        LeafPage prevPage = loadLeafPage(idxFileInfo, page.getPrevPageNo());
        if (prevPage != null &&
            prevPage.getParentPageNo() == page.getParentPageNo()) {

            // See if we can move some of this leaf's entries to the previous
            // leaf, to free up space.

            int count = tryLeafRelocateForSpace(page, prevPage, bytesRequired);
            if (count > 0) {
                // Yes, we can do it!
                
                logger.debug(String.format("Relocating %d entries from " +
                    "leaf-page %d to left-sibling leaf-page %d", count,
                    page.getPageNo(), prevPage.getPageNo()));
                
                page.moveEntriesLeft(prevPage, count);
                BTreeIndexPageTuple firstRightKey =
                    addEntryToLeafPair(prevPage, page, key);

                DBPage parentPage = storageManager.loadDBPage(dbFile,
                    page.getParentPageNo());
                NonLeafPage nonLeafPage = new NonLeafPage(parentPage, idxFileInfo);
                updateEntryInNonLeafPage(nonLeafPage, prevPage.getPageNo(),
                    firstRightKey, page.getPageNo());

                return true;
            }
        }

        LeafPage nextPage = loadLeafPage(idxFileInfo, page.getNextPageNo());
        if (nextPage != null &&
            nextPage.getParentPageNo() == page.getParentPageNo()) {

            // See if we can move some of this leaf's entries to the next leaf,
            // to free up space.

            int count = tryLeafRelocateForSpace(page, nextPage, bytesRequired);
            if (count > 0) {
                // Yes, we can do it!

                logger.debug(String.format("Relocating %d entries from " +
                    "leaf-page %d to right-sibling leaf-page %d", count,
                    page.getPageNo(), nextPage.getPageNo()));

                page.moveEntriesRight(nextPage, count);
                BTreeIndexPageTuple firstRightKey =
                    addEntryToLeafPair(page, nextPage, key);

                DBPage parentPage = storageManager.loadDBPage(dbFile,
                    page.getParentPageNo());
                NonLeafPage nonLeafPage = new NonLeafPage(parentPage, idxFileInfo);
                updateEntryInNonLeafPage(nonLeafPage, page.getPageNo(),
                    firstRightKey, nextPage.getPageNo());

                return true;
            }
        }

        // Couldn't relocate entries to either the prevous or next page.  We
        // must split the leaf into two.
        return false;
    }

    
    private void updateEntryInNonLeafPage(NonLeafPage page, int prevPageNo,
                                          Tuple key, int nextPageNo) {
        for (int i = 0; i < page.getNumPointers() - 1; i++) {
            if (page.getPointer(i) == prevPageNo &&
                page.getPointer(i + 1) == nextPageNo) {

                page.replaceKey(i, key);
                return;
            }
        }
        
        for (int i = 0; i < page.getNumPointers(); i++) {
            logger.error(String.format("Page %d pointer %d value is %d",
                page.getPageNo(), i, page.getPointer(i)));
        }
        
        
        throw new IllegalStateException(
            "Couldn't find sequence of page-pointers [" + prevPageNo + ", " +
            nextPageNo + "] in non-leaf page " + page.getPageNo());
    }
    

    /**
     * This helper function determines how many entries must be relocated from
     * one leaf-page to another, in order to free up the specified number of
     * bytes.  If it is possible, the number of entries that must be relocated
     * is returned.  If it is not possible, the method returns 0.
     *
     * @param leaf the leaf node to relocate entries from
     *
     * @param adjLeaf the adjacent leaf (predecessor or successor) to relocate
     *        entries to
     *
     * @param bytesRequired the number of bytes that must be freed up in
     *        {@code leaf} by the operation
     *
     * @return the number of entries that must be relocated to free up the
     *         required space, or 0 if it is not possible.
     */
    private int tryLeafRelocateForSpace(LeafPage leaf, LeafPage adjLeaf,
                                        int bytesRequired) {

        // TODO:  BIG BUG!  Sometimes records are moved from the end of the
        //        leaf, sometimes they are moved from the start.

        int leafBytesFree = leaf.getFreeSpace();
        int adjBytesFree = adjLeaf.getFreeSpace();

        int numRelocated = 0;
        while (true) {
            int keySize = leaf.getKeySize(numRelocated);

            if (adjBytesFree < keySize)
                break;

            numRelocated++;

            leafBytesFree += keySize;
            adjBytesFree -= keySize;

            // Since we don't yet know which leaf the new key will go into,
            // stop when we can put the key in either leaf.
            if (leafBytesFree >= bytesRequired &&
                adjBytesFree >= bytesRequired) {
                break;
            }
        }

        return numRelocated;
    }


    /**
     * This helper method takes a pair of leaf nodes that are siblings to each
     * other, and adds the specified key to whichever leaf the key should go
     * into.  The method returns the first key in the right leaf-page, since
     * this value is necessary to update the parent node of the pair of leaves.
     *
     * @param prevLeaf the first leaf in the pair, left sibling of
     *        {@code nextLeaf}
     *
     * @param nextLeaf the second leaf in the pair, right sibling of
     *        {@code prevLeaf}
     *
     * @param key the key to insert into the pair of leaves
     *
     * @return the first key of {@code nextLeaf}, after the insert is completed
     */
    private BTreeIndexPageTuple addEntryToLeafPair(LeafPage prevLeaf,
        LeafPage nextLeaf, TupleLiteral key) {

        BTreeIndexPageTuple firstRightKey = nextLeaf.getKey(0);
        if (TupleComparator.compareTuples(key, firstRightKey) < 0) {
            // The new key goes in the left page.
            prevLeaf.addEntry(key);
        }
        else {
            // The new key goes in the right page.
            nextLeaf.addEntry(key);

            // Re-retrieve the right page's first key since it may have changed.
            firstRightKey = nextLeaf.getKey(0);
        }

        return firstRightKey;
    }
    

    private void splitLeafAndAddKey(LeafPage leaf, TupleLiteral key)
        throws IOException {

        if (logger.isDebugEnabled()) {
            logger.debug("Splitting leaf-page " + leaf.getPageNo() +
                " into two leaves.");
            logger.debug("    Old prev-page:  " + leaf.getPrevPageNo() +
                "    Old next-page:  " + leaf.getNextPageNo());
        }

        // Get a new blank page in the index, with the same parent as the
        // leaf-page we were handed.

        IndexFileInfo idxFileInfo = leaf.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage newPage = getNewDataPage(idxFileInfo.getDBFile());
        LeafPage newLeaf = LeafPage.init(newPage, idxFileInfo);

        // We may need to update the details in the header page.
        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Chain the leaf-page into the sequence with the previous and next
        // leaves.  The new leaf always follows the leaf we were handed.
        newLeaf.setPrevPageNo(leaf.getPageNo());
        newLeaf.setNextPageNo(leaf.getNextPageNo());
        leaf.setNextPageNo(newLeaf.getPageNo());

        if (logger.isDebugEnabled()) {
            logger.debug("    New prev-page:  " + leaf.getPrevPageNo() +
                "    New next-page:  " + leaf.getNextPageNo());
            logger.debug("    New next-leaf prev-page:  " + newLeaf.getPrevPageNo() +
                "    New next-leaf next-page:  " + newLeaf.getNextPageNo());
        }

        if (HeaderPage.getLastLeafPageNo(dbpHeader) == leaf.getPageNo()) {
            // The "last page" in the index is changing.
            HeaderPage.setLastLeafPageNo(dbpHeader, newLeaf.getPageNo());
        }
        
        // Set the new leaf to have the same parent-page as the current leaf,
        // even though we may have to create a new parent-node later on.  This
        // is so we can move entries from the old leaf to the new leaf.
        int parentPageNo = leaf.getParentPageNo();
        newLeaf.setParentPageNo(parentPageNo);

        // Figure out how many values we want to move from the old leaf to the
        // new leaf.
        
        int numEntries = leaf.getNumEntries();

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Relocating %d entries from left-leaf %d" +
                " to right-leaf %d", numEntries, leaf.getPageNo(), newLeaf.getPageNo()));
            logger.debug("    Old left # of entries:  " + leaf.getNumEntries());
            logger.debug("    Old right # of entries:  " + newLeaf.getNumEntries());
        }

        leaf.moveEntriesRight(newLeaf, numEntries / 2);

        if (logger.isDebugEnabled()) {
            logger.debug("    New left # of entries:  " + leaf.getNumEntries());
            logger.debug("    New right # of entries:  " + newLeaf.getNumEntries());
        }

        BTreeIndexPageTuple firstRightKey =
            addEntryToLeafPair(leaf, newLeaf, key);

        // If the current leaf doesn't have a parent, it's because it's
        // currently the root.
        if (parentPageNo == 0) {
            // Create a new root node and set both leaves to have it as their
            // parent.
            DBPage parentPage = getNewDataPage(dbFile);
            NonLeafPage.init(parentPage, idxFileInfo,
                leaf.getPageNo(), firstRightKey, newLeaf.getPageNo());

            parentPageNo = parentPage.getPageNo();

            leaf.setParentPageNo(parentPageNo);
            newLeaf.setParentPageNo(parentPageNo);
            
            // We have a new root-page in the index!
            HeaderPage.setRootPageNo(dbpHeader, parentPageNo);
        }
        else {
            // Add the new leaf into the parent non-leaf node.  (This may cause
            // the parent node's contents to be moved or split, if the parent
            // is full.)

            // (We already set the new leaf's parent-page-number earlier.)

            DBPage dbpParent = storageManager.loadDBPage(dbFile, parentPageNo);
            NonLeafPage parentPage = new NonLeafPage(dbpParent, idxFileInfo);
            addEntryToNonLeafPage(parentPage, leaf.getPageNo(), firstRightKey,
                newLeaf.getPageNo());
            
            logger.debug("Parent page " + parentPageNo + " now has " +
                parentPage.getNumPointers() + " page-pointers.");
        }
    }
    
    
    private void addEntryToNonLeafPage(NonLeafPage page, int prevPageNo,
        Tuple key, int nextPageNo) throws IOException {

        // The new entry will be the key, plus 2 bytes for the page-pointer.
        List<ColumnInfo> colInfos = page.getIndexFileInfo().getIndexSchema();
        int newEntrySize = PageTuple.getTupleStorageSize(colInfos, key) + 2;

        if (page.getFreeSpace() < newEntrySize) {
            // Try to relocate entries from this inner page to either sibling,
            // or if that can't happen, split the inner page into two.
            /*
            if (!relocateNonLeafEntriesAndAddKey(page, prevPageNo, key,
                                                 nextPageNo, newEntrySize)) {
                splitNonLeafAndAddKey(page, prevPageNo, key, nextPageNo);
            }
            */
            throw new UnsupportedOperationException("Not yet implemented");
        }
        else {
            // There is room in the leaf for the new key.  Add it there.
            page.addEntry(prevPageNo, key, nextPageNo);
        }

    }

/*
    private boolean relocateNonLeafEntriesAndAddKey(NonLeafPage page,
        int prevPageNo, Tuple key, int nextPageNo, int bytesRequired)
        throws IOException {

        // See if we are able to relocate records either direction to free up
        // space for the new key.

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();

        NonLeafPage prevPage = loadNonLeafPage(idxFileInfo, page.getPrevPageNo());
        if (prevPage != null &&
            prevPage.getParentPageNo() == page.getParentPageNo()) {

            // See if we can move some of this leaf's entries to the previous
            // leaf, to free up space.

            int count = tryNonLeafRelocateForSpace(page, prevPage, bytesRequired);
            if (count > 0) {
                // Yes, we can do it!

                // TODO:  Need parent's key too.
                page.moveEntriesLeft(prevPage, count);
                BTreeIndexPageTuple firstRightKey =
                    addEntryToLeafPair(prevPage, page, key);

                DBPage parentPage = storageManager.loadDBPage(dbFile,
                    page.getParentPageNo());
                NonLeafPage nonLeafPage = new NonLeafPage(parentPage, idxFileInfo);
                updateEntryInNonLeafPage(nonLeafPage, prevPage.getPageNo(),
                    firstRightKey, page.getPageNo());

                return true;
            }
        }

        NonLeafPage nextPage = loadNonLeafPage(idxFileInfo, page.getNextPageNo());
        // TODO:  Try to relocate right.

        // Couldn't relocate entries to either the previous or next page.  We
        // must split the leaf into two.
        return false;
    }
*/


    @Override
    public void deleteTuple(IndexFileInfo idxFileInfo, PageTuple tup)
        throws IOException {
        // TODO:  IMPLEMENT
    }


    /**
     * This helper function creates a {@link TupleLiteral} that holds the
     * key-values necessary for storing or deleting the specified table-tuple
     * in the index.  For making a lookup-only key value, use the
     * {@link #makeLookupKeyValue} function.  The difference between the two
     * functions is that this version stores the file-pointer from the tuple
     * into the key as the last value, but the {@link #makeLookupKeyValue}
     * helper writes a dummy [0, 0] file-pointer into the key.
     *
     * @param idxFileInfo the details of the index that the key will be created
     *                    for
     *
     * @param ptup the tuple from the original table, that the key will be
     *        created from.
     *
     * @return a tuple-literal that can be used for storing, looking up, or
     *         deleting the specific tuple {@code ptup}.
     */
    private TupleLiteral makeStoredKeyValue(IndexFileInfo idxFileInfo,
                                                 PageTuple ptup) {

        // Figure out what columns from the table we use for the index keys.
        ColumnIndexes colIndexes = idxFileInfo.getTableColumnIndexes();
        
        // Build up a new tuple-literal containing the new key to be inserted.
        TupleLiteral newKeyVal = new TupleLiteral();
        for (int i = 0; i < colIndexes.size(); i++)
            newKeyVal.addValue(ptup.getColumnValue(colIndexes.getCol(i)));

        // Include the file-pointer as the last value in the tuple, so that all
        // key-values are unique in the index.
        newKeyVal.addValue(ptup.getExternalReference());

        List<ColumnInfo> colInfos = idxFileInfo.getIndexSchema();
        int storageSize = PageTuple.getTupleStorageSize(colInfos, newKeyVal);
        newKeyVal.setStorageSize(storageSize);

        return newKeyVal;
    }


    private TupleLiteral makeLookupKeyValue(Tuple tup) {
        TupleLiteral lookupVal = new TupleLiteral(tup);

        // Put a dummy file-pointer on the end of the lookup tuple.
        lookupVal.addValue(FilePointer.ZERO_FILE_POINTER);

        return lookupVal;
    }
}
