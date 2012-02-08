package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.storage.heapfile.DataPage;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.indexes.IndexPointer;

import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.PageWriter;
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


    public static final String IDXNAME_UNNAMED_PK = "PK_%s";


    public static final String IDXNAME_UNNAMED_CK = "CK_%s";


    public static final String IDXNAME_UNNAMED_INDEX = "IDX_%s";


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
    public void addTuple(IndexFileInfo idxFileInfo, TableSchema schema,
        ColumnIndexes colIndexes, PageTuple tup) throws IOException {

        // Pull out the index name so we can use it for helpful log messages.
        String indexName = idxFileInfo.getIndexName();
        
        // Get the schema of the index so that we can interpret the key-values.
        ArrayList<ColumnInfo> colInfos = schema.getColumnInfos(colIndexes);
        colInfos.add(new ColumnInfo("#TUPLE_FP",
            new ColumnType(SQLDataType.FILE_POINTER)));

        // The header page tells us where the root page starts.
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // These are the values we store into the index for the tuple:  the key,
        // and a file-pointer to the tuple that the key is for.
        TupleLiteral newTupleKey = makeStoredKeyValue(tup, colIndexes);
//        FilePointer newTupleFilePtr = tup.getFilePointer();

        logger.debug("Adding search-key value " + newTupleKey + " to index " +
            indexName);

        // Get the root page of the index.
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        DBPage dbpRoot = null;

        if (rootPageNo == 0) {
            // The index doesn't have any data-pages at all yet; we need to
            // create a brand new leaf page and make it the root.

            logger.debug("Index " + indexName + " currently has no data " +
                "pages; finding/creating one to use as the root!");

            dbpRoot = getNewDataPage(dbFile, dbpHeader);
            rootPageNo = dbpRoot.getPageNo();

            HeaderPage.setRootPageNo(dbpHeader, rootPageNo);
            HeaderPage.setFirstLeafPageNo(dbpHeader, rootPageNo);
            HeaderPage.setLastLeafPageNo(dbpHeader, rootPageNo);

            dbpRoot.writeByte(0, BTREE_LEAF_PAGE);
            LeafPage.init(dbpRoot, colInfos);

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

            int numKeys = NonLeafPage.numKeys(dbPage);
            BTreeIndexPageTuple key = NonLeafPage.getFirstKey(dbPage, colInfos);
            for (int i = 0; i < numKeys; i++) {
                int cmp = NonLeafPage.compareToKey(newTupleKey, key);
                if (cmp < 0) {
                    logger.debug("Value is less than key at index " + i +
                        "; following pointer " + i + " before this key.");
                    nextPageNo = NonLeafPage.getPointerBeforeKey(key);
                    break;
                }
                else if (cmp == 0) {
                    logger.debug("Value is equal to key at index " + i +
                        "; following pointer " + (i+1) + " after this key.");
                    nextPageNo = NonLeafPage.getPointerAfterKey(key);
                    break;
                }

                // Go on to the next key.
                key = NonLeafPage.getNextKey(key);
            }
            if (nextPageNo == -1) {
                // None of the other entries in the page matched the tuple.  Get
                // the last pointer in the page.
                logger.debug("Value is greater than all keys in this page;" +
                    " following last pointer " + numKeys + " in the page.");
                nextPageNo = NonLeafPage.getPointerAfterKey(key);
            }

            // Navigate to the next page in the index.
            dbPage = storageManager.loadDBPage(dbFile, nextPageNo);
            pageType = dbPage.readByte(0);
            if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
                throw new IOException("Invalid page type encountered:  " + pageType);
        }

        // Now we are at a leaf page, we can figure out where the next value
        // goes.

        int newEntrySize = PageTuple.getTupleStorageSize(colInfos, newTupleKey);
        newEntrySize += 4;  // Include the size of the file-pointer as well.

        LeafPage leaf = new LeafPage(dbPage, colInfos);
        if (leaf.getFreeSpace() < newEntrySize) {
            // TODO:  Split the leaf page into two leaves.
            throw new UnsupportedOperationException(
                "NYI:  Leaf page is full and I don't know how to split it yet!");
        }

        // Insert the key and its corresponding tuple-pointer into the page,
        // making sure to keep the keys in increasing order.

        int numEntries = leaf.getNumEntries();
        if (numEntries == 0) {
            logger.debug("Leaf page is empty; storing new entry at start.");
            leaf.addEntryAtIndex(newTupleKey, 0);
        }
        else {
            int i;
            for (i = 0; i < numEntries; i++) {
                BTreeIndexPageTuple key = leaf.getKey(i);

                logger.debug(i + ":  comparing " + newTupleKey + " to " + key);
                
                // Compare the tuple to the current key.  Once we find where the
                // new key/tuple should go, copy the key/pointer into the page.
                if (LeafPage.compareToKey(newTupleKey, key) < 0) {
                    logger.debug("Storing new entry at index " + i +
                        " in the leaf page.");
                    leaf.addEntryAtIndex(newTupleKey, i);
                    break;
                }
            }

            if (i == numEntries) {
                // The new tuple will go at the end of this page's entries.
                logger.debug("Storing new entry at end of leaf page.");
                leaf.addEntryAtIndex(newTupleKey, numEntries);
            }
        }
    }


    /**
     * This helper function finds and returns a new data page, either by taking
     * it from the empty-pages list in the index file, or if this list is empty,
     * creating a brand new page at the end of the file.
     *
     * @param dbFile the index file to get a new empty data page from
     * @param dbpHeader the header page of the index file, which is usually
     *        already loaded before this method is called, and which sometimes
     *        needs to be updated if a page is taken from the free list.
     *
     * @return an empty {@code DBPage} that can be used as a new index page.
     *
     * @throws IOException if an error occurs while loading a data page, or
     *         while extending the size of the index file.
     */
    private DBPage getNewDataPage(DBFile dbFile, DBPage dbpHeader)
        throws IOException {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (dbpHeader == null)
            throw new IllegalArgumentException("dbpHeader cannot be null");
        
        if (dbpHeader.getPageNo() != 0) {
            throw new IllegalArgumentException("dbpHeader must be the header " +
                "page of the index file; got page-no. " + dbpHeader.getPageNo());
        }

        DBPage newPage = null;
        int pageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);
        if (pageNo == 0) {
            // There are no empty pages.  Create a new page to use.
            newPage =
                storageManager.loadDBPage(dbFile, dbFile.getNumPages(), true);
        }
        else {
            // Load the empty page, and remove it from the chain of empty pages.
            newPage = storageManager.loadDBPage(dbFile, pageNo);
            HeaderPage.setFirstEmptyPageNo(dbpHeader, newPage.readUnsignedShort(1));
        }

        return newPage;
    }


    @Override
    public void deleteTuple(IndexFileInfo idxFileInfo, Tuple tup) throws IOException {
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
     * @param ptup the tuple from the original table, that the key will be
     *        created from.
     *
     * @param colIndexes the column-indexes of the tuple to use for constructing
     *                   the key
     *
     * @return a tuple-literal that can be used for storing, looking up, or
     *         deleting the specific tuple {@code ptup}.
     */
    private TupleLiteral makeStoredKeyValue(PageTuple ptup,
                                            ColumnIndexes colIndexes) {
        // Build up a new tuple-literal containing the new key to be inserted.
        TupleLiteral newKeyVal = new TupleLiteral();
        for (int i = 0; i < colIndexes.size(); i++)
            newKeyVal.addValue(ptup.getColumnValue(colIndexes.getCol(i)));
        
        // Include the file-pointer as the last value in the tuple, so that all
        // key-values are unique in the index.
        newKeyVal.addValue(ptup.getExternalReference());

        return newKeyVal;
    }


    private TupleLiteral makeLookupKeyValue(Tuple tup) {
        TupleLiteral lookupVal = new TupleLiteral(tup);

        // Put a dummy file-pointer on the end of the lookup tuple.
        lookupVal.addValue(FilePointer.ZERO_FILE_POINTER);

        return lookupVal;
    }
}
