package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;

import java.util.ArrayList;
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
 *     the {@link InnerPage} and {@link LeafPage} classes for details.)
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
     * If this flag is set to true, all data in data-pages that is no longer
     * necessary is cleared.  This will increase the cost of write-ahead
     * logging, but it also exposes bugs more quickly because old data won't be
     * around.
     */
    public static final boolean CLEAR_OLD_DATA = true;


    /**
     * The table manager uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;
    
    
    private LeafPageOperations leafPageOps;
    
    
    private InnerPageOperations innerPageOps;


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

        innerPageOps = new InnerPageOperations(this);
        leafPageOps = new LeafPageOperations(this, innerPageOps);
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
        ArrayList<Integer> pagePath = new ArrayList<Integer>();
        LeafPage leaf =
            navigateToLeafPage(idxFileInfo, newTupleKey, true, pagePath);

        leafPageOps.addEntry(leaf, newTupleKey, pagePath);
    }


    @Override
    public void deleteTuple(IndexFileInfo idxFileInfo, PageTuple tup)
        throws IOException {
        // TODO:  IMPLEMENT
    }




    private LeafPage navigateToLeafPage(IndexFileInfo idxFileInfo,
        TupleLiteral searchKey, boolean createIfNeeded,
        List<Integer> pagePath) throws IOException {

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

        if (pagePath != null)
            pagePath.add(rootPageNo);

        while (pageType != BTREE_LEAF_PAGE) {
            logger.debug("Examining non-leaf page " + dbPage.getPageNo() +
                " of index " + indexName);

            int nextPageNo = -1;
            
            InnerPage innerPage = new InnerPage(dbPage, idxFileInfo);

            int numKeys = innerPage.getNumKeys();
            if (numKeys < 1) {
                throw new IllegalStateException("Non-leaf page " +
                    dbPage.getPageNo() + " is invalid:  it contains no keys!");
            }

            for (int i = 0; i < numKeys; i++) {
                BTreeIndexPageTuple key = innerPage.getKey(i);
                int cmp = TupleComparator.compareTuples(searchKey, key);
                if (cmp < 0) {
                    logger.debug("Value is less than key at index " + i +
                        "; following pointer " + i + " before this key.");
                    nextPageNo = innerPage.getPointer(i);
                    break;
                }
                else if (cmp == 0) {
                    logger.debug("Value is equal to key at index " + i +
                        "; following pointer " + (i+1) + " after this key.");
                    nextPageNo = innerPage.getPointer(i + 1);
                    break;
                }
            }

            if (nextPageNo == -1) {
                // None of the other entries in the page matched the tuple.  Get
                // the last pointer in the page.
                logger.debug("Value is greater than all keys in this page;" +
                    " following last pointer " + numKeys + " in the page.");
                nextPageNo = innerPage.getPointer(numKeys);
            }

            // Navigate to the next page in the index.
            dbPage = storageManager.loadDBPage(dbFile, nextPageNo);
            pageType = dbPage.readByte(0);
            if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
                throw new IOException("Invalid page type encountered:  " + pageType);

            if (pagePath != null)
                pagePath.add(nextPageNo);
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
    public DBPage getNewDataPage(DBFile dbFile) throws IOException {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        DBPage newPage;
        int pageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);

        if (pageNo == 0) {
            // There are no empty pages.  Create a new page to use.

            logger.debug("No empty pages.  Extending index file " + dbFile +
                " by one page.");

            int numPages = dbFile.getNumPages();
            newPage = storageManager.loadDBPage(dbFile, numPages, true);
        }
        else {
            // Load the empty page, and remove it from the chain of empty pages.

            logger.debug("First empty page number is " + pageNo);

            newPage = storageManager.loadDBPage(dbFile, pageNo);
            int nextEmptyPage = newPage.readUnsignedShort(1);
            HeaderPage.setFirstEmptyPageNo(dbpHeader, nextEmptyPage);
        }

        logger.debug("Found new data page for the index:  page " +
            newPage.getPageNo());

        // TODO:  Increment the number of data pages?

        return newPage;
    }


    /**
     * This helper function marks a data page in the index as "empty", and adds
     * it to the list of empty pages in the index file.
     *
     * @param dbPage the data-page that is no longer used.
     *
     * @throws IOException if an IO error occurs while releasing the data page,
     *         such as not being able to load the header page.
     */
    public void releaseDataPage(DBPage dbPage) throws IOException {
        // TODO:  If this page is the last page of the index file, we could
        //        truncate pages off the end until we hit a non-empty page.
        //        Instead, we'll leave all the pages around forever...

        DBFile dbFile = dbPage.getDBFile();
        
        // Record in the page that it is empty.
        dbPage.writeByte(0, BTREE_EMPTY_PAGE);

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Retrieve the old "first empty page" value, and store it in this page.
        int prevEmptyPageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);
        dbPage.writeShort(1, prevEmptyPageNo);

        if (CLEAR_OLD_DATA) {
            // Clear out the remainder of the data-page since it's now unused.
            dbPage.setDataRange(3, dbPage.getPageSize() - 3, (byte) 0);
        }

        // Store the new "first empty page" value into the header.
        HeaderPage.setFirstEmptyPageNo(dbpHeader, dbPage.getPageNo());
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


    /**
     * This helper function creates a {@link TupleLiteral} that holds the
     * key-values necessary for looking up a table-tuple in the index.  For
     * making a key value for storing or deleting a tuple, use the
     * {@link #makeStoredKeyValue} function.  The difference between the two
     * functions is that this version stores a dummy [0, 0] file-pointer into
     * the key as the last value, but the {@link #makeStoredKeyValue} helper
     * writes the page-tuple's file-pointer into the key.
     *
     * @param tup the tuple that the key will be created from.
     *
     * @return a tuple-literal that can be used for looking up tuples with the
     *         key-values stored in {@code tup}.
     */
    private TupleLiteral makeLookupKeyValue(Tuple tup) {
        TupleLiteral lookupVal = new TupleLiteral(tup);

        // Put a dummy file-pointer on the end of the lookup tuple.
        lookupVal.addValue(FilePointer.ZERO_FILE_POINTER);

        return lookupVal;
    }
}
