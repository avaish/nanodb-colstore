package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.indexes.IndexPointer;
import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.PageTuple;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexManager;

import edu.caltech.nanodb.relations.TableConstraintType;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
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

        // The index's header page mainly stores what columns are in the index,
        // and also the roots of the indexing structure.  The actual schema
        // information is stored in the referenced table.  Thus, there isn't a
        // whole lot of information to store in the index header.
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);

        headerPage.writeShort(HeaderPage.OFFSET_NUM_DATA_PAGES, 0);

        PageWriter hpWriter = new PageWriter(headerPage);
        hpWriter.setPosition(HeaderPage.OFFSET_INDEX_SPEC);

        // Write out the index information.
        // TODO:  logger.info("Writing index specification");
    }


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


    /**
     * This method reads in the schema and other critical information for the
     * specified table.
     *
     * @throws IOException if an IO error occurs when attempting to load the
     *         table's schema and other details.
     */
    public void loadIndexInfo(IndexFileInfo idxFileInfo) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented.");
    }


    @Override
    public IndexPointer addTuple(IndexFileInfo idxFileInfo, TableSchema schema,
        ColumnIndexes colIndexes, PageTuple tup) throws IOException {
        
        // Get the schema of the index so that we can interpret the key-values.
        List<ColumnInfo> colInfos = schema.getColumnInfos(colIndexes);

        // The header page tells us where the root page starts.
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);

        // Get the root page of the index.
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        DBPage dbpRoot = storageManager.loadDBPage(dbFile, rootPageNo);

        // Navigate through the page hierarchy until we reach a leaf page.

        TupleLiteral newTupleKey = makeKeyValue(tup, colIndexes);
        FilePointer newTupleFilePtr = tup.getFilePointer();

        DBPage dbPage = dbpRoot;
        int pageType = dbPage.readByte(0);
        if (pageType != BTREE_INNER_PAGE && pageType != BTREE_LEAF_PAGE)
            throw new IOException("Invalid page type encountered:  " + pageType);

        while (pageType != BTREE_LEAF_PAGE) {
            int nextPageNo = -1;

            int numKeys = NonLeafPage.numKeys(dbPage);
            BTreeIndexPageTuple key = NonLeafPage.getFirstKey(dbPage, colInfos);
            for (int i = 0; i < numKeys; i++) {
                if (NonLeafPage.compareToKey(newTupleKey, null, key, null) < 0)
                    nextPageNo = NonLeafPage.getPointerBeforeKey(key);

                key = NonLeafPage.getNextKey(key);
            }
            if (nextPageNo == -1) {
                // None of the other entries in the page matched the tuple.  Get
                // the last pointer in the page.
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
        }

        // Insert the key and its corresponding tuple-pointer into the page.
        {
            int numEntries = leaf.getNumEntries();
            int i;
            for (i = 0; i < numEntries; i++) {
                BTreeIndexPageTuple key = leaf.getKey(i);

                // Compare the tuple to the current key.  Once we find where the
                // new key/tuple should go, copy the key/pointer into the page.
                if (LeafPage.compareToKey(newTupleKey, newTupleFilePtr,
                                          key, null) >= 0) {
                    leaf.addEntryAtIndex(newTupleKey, newTupleFilePtr, i);
                    break;
                }
            }

            if (i == numEntries) {
                // The new tuple will go at the end of this page's entries.
                leaf.addEntryAtIndex(newTupleKey, newTupleFilePtr, numEntries);
            }
        }

        return new IndexPointer(newTupleFilePtr);
    }


    @Override
    public void deleteTuple(IndexFileInfo idxFileInfo, Tuple tup) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
    
    
    private TupleLiteral makeKeyValue(Tuple tblTuple, ColumnIndexes colIndexes) {
        // Build up a new tuple-literal containing the new key to be inserted.
        TupleLiteral newKeyVal = new TupleLiteral();
        for (int i = 0; i < colIndexes.size(); i++)
            newKeyVal.addValue(tblTuple.getColumnValue(colIndexes.getCol(i)));

        return newKeyVal;
    }
}
