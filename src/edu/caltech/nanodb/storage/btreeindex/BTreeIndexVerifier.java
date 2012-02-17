package edu.caltech.nanodb.storage.btreeindex;

import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * This class provides some simple verification operations for B<sup>+</sup>
 * trees.
 */
public class BTreeIndexVerifier {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BTreeIndexVerifier.class);

    
    private static class ScanAbortedException extends RuntimeException {
        // Nothing.
    }
    

    /**
     * This helper class is used to keep track of details of pages within the
     * B<sup>+</sup> tree index.
     */
    private static class PageInfo {
        /** The page's number. */
        public int pageNo;


        /** The type of the index page. */
        public int pageType;


        /** A flag indicating whether the page is accessible from the root. */
        public boolean accessibleFromRoot;


        /**
         * Records how many times the page has been referenced in the index's
         * tree structure.
         */
        public int numTreeReferences;


        /**
         * Records how many times the page has been referenced in the index's
         * leaf page list.
         */
        public int numLeafListReferences;


        /**
         * Records how many times the page has been referenced in the index's
         * empty page list.
         */
        public int numEmptyListReferences;


        public PageInfo(int pageNo, int pageType) {
            this.pageNo = pageNo;
            this.pageType = pageType;

            accessibleFromRoot = false;
            numTreeReferences = 0;
            numLeafListReferences = 0;
            numEmptyListReferences = 0;
        }
    }


    private StorageManager storageManager;
    
    
    private IndexFileInfo idxFileInfo;

    
    private DBFile dbFile;
    

    private HashMap<Integer, PageInfo> pages;


    private ArrayList<String> errors;

    
    
    public BTreeIndexVerifier(IndexFileInfo idxFileInfo) {
        storageManager = StorageManager.getInstance();
        this.idxFileInfo = idxFileInfo;
        dbFile = idxFileInfo.getDBFile();
    }
    
    
    public List<String> verify() throws IOException {
        errors = new ArrayList<String>();

        try {
            pass1ScanThruAllPages();
            pass2TreeScanThruIndex();
            pass3ScanThruLeafList();
            pass4ScanThruEmptyList();
            pass5ExaminePageReachability();
        }
        catch (ScanAbortedException e) {
            // Do nothing.
            logger.warn("Index verification scan aborted.");
        }

        return errors;
    }


    /**
     * This method implements pass 1 of the verification process:  scanning
     * through all pages in the index file, collecting basic details about
     * each page.
     *
     * @throws IOException if an IO error occurs during this process
     */
    private void pass1ScanThruAllPages() throws IOException {
        logger.debug("Pass 1:  Linear scan through pages to collect info");

        pages = new HashMap<Integer, PageInfo>();
        for (int pageNo = 1; pageNo < dbFile.getNumPages(); pageNo++) {
            DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);

            int pageType = dbPage.readUnsignedByte(0);
            PageInfo info = new PageInfo(pageNo, pageType);
            pages.put(pageNo, info);
        }
    }


    /**
     * This method implements pass 2 of the verification process:  performing a
     * tree-scan through the entire index, starting with the root page of the
     * index.
     *
     * @throws IOException if an IO error occurs during this process
     */
    private void pass2TreeScanThruIndex() throws IOException {
        logger.debug("Pass 2:  Tree scan from root to verify nodes");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);

        scanTree(rootPageNo, 0, null, null);
    }


    private void scanTree(int pageNo, int parentPageNo,
                          Tuple parentLeftKey, Tuple parentRightKey)
        throws IOException {

        PageInfo info = pages.get(pageNo);
        info.accessibleFromRoot = true;
        info.numTreeReferences++;

        if (info.numTreeReferences > 10) {
            errors.add(String.format("Pass 2:  Stopping scan!  I've visited " +
                "page %d %d times; there may be a loop in your index structure.",
                pageNo, info.numTreeReferences));
            throw new ScanAbortedException();
        }

        logger.trace("Examining page " + pageNo);
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);

        switch (info.pageType) {
        case BTreeIndexManager.BTREE_INNER_PAGE:
        {
            logger.trace("It's an inner page.");
            InnerPage inner = new InnerPage(dbPage, idxFileInfo);

            ArrayList<Integer> refPages = new ArrayList<Integer>();
            int refInner = 0;
            int refLeaf = 0;
            int refOther = 0;

            // Check the pages referenced from this page using the basic info
            // collected in Pass 1.

            for (int p = 0; p < inner.getNumPointers(); p++) {
                int refPageNo = inner.getPointer(p);
                refPages.add(refPageNo);
                PageInfo refPageInfo = pages.get(refPageNo);

                switch (refPageInfo.pageType) {
                case BTreeIndexManager.BTREE_INNER_PAGE:
                    refInner++;
                    break;

                case BTreeIndexManager.BTREE_LEAF_PAGE:
                    refLeaf++;
                    break;

                default:
                    refOther++;
                }

                if (refInner != 0 && refLeaf != 0) {
                    errors.add(String.format("Pass 2:  Inner page %d " +
                        "references both inner and leaf pages.", pageNo));
                }

                if (refOther != 0) {
                    errors.add(String.format("Pass 2:  Inner page %d references " +
                        "pages that are neither inner pages nor leaf pages.", pageNo));
                }
            }

            // Make sure the keys are in the proper order in the page.

            int numKeys = inner.getNumKeys();
            ArrayList<TupleLiteral> keys = new ArrayList<TupleLiteral>(numKeys);
            if (numKeys > 1) {
                Tuple prevKey = inner.getKey(0);
                keys.add(new TupleLiteral(prevKey));

                if (parentLeftKey != null) {
                    int cmp = TupleComparator.compareTuples(parentLeftKey, prevKey);
                    // It is possible that the parent's left-key would be the
                    // same as the first key in this page.
                    if (cmp > 0) {
                        errors.add(String.format("Pass 2:  Parent page %d's " +
                            "left key is greater than inner page %d's first key",
                            parentPageNo, pageNo));
                    }
                }

                for (int k = 1; k < numKeys; k++) {
                    Tuple key = inner.getKey(k);
                    keys.add(new TupleLiteral(key));

                    int cmp = TupleComparator.compareTuples(prevKey, key);
                    if (cmp == 0) {
                        errors.add(String.format("Pass 2:  Inner page %d keys " +
                            "%d and %d are duplicates!", pageNo, k - 1, k));
                    }
                    else if (cmp > 0) {
                        errors.add(String.format("Pass 2:  Inner page %d keys " +
                            "%d and %d are out of order!", pageNo, k - 1, k));
                    }
                    prevKey = key;
                }

                if (parentRightKey != null) {
                    int cmp = TupleComparator.compareTuples(prevKey, parentRightKey);
                    // The parent's right-key should be greater than the last
                    // key in this page.
                    if (cmp >= 0) {
                        errors.add(String.format("Pass 2:  Parent page %d's " +
                            "right key is less than or equal to inner page " +
                            "%d's last key", parentPageNo, pageNo));
                    }
                }
            }
            
            // Now that we are done with this page, check each child-page.

            int p = 0;
            Tuple prevKey = parentLeftKey;
            for (int refPageNo : refPages) {
                Tuple nextKey;
                if (p < keys.size())
                    nextKey = keys.get(p);
                else
                    nextKey = parentRightKey;

                scanTree(refPageNo, pageNo, prevKey, nextKey);
                prevKey = nextKey;
                p++;
            }

            break;
        }

        case BTreeIndexManager.BTREE_LEAF_PAGE:
        {
            logger.trace("It's a leaf page.");
            LeafPage leaf = new LeafPage(dbPage, idxFileInfo);

            // Make sure the keys are in the proper order in the page.

            int numKeys = leaf.getNumEntries();
            if (numKeys >= 1) {
                Tuple prevKey = leaf.getKey(0);

                if (parentLeftKey != null) {
                    int cmp = TupleComparator.compareTuples(parentLeftKey, prevKey);
                    // It is possible that the parent's left-key would be the
                    // same as the first key in this page.
                    if (cmp > 0) {
                        errors.add(String.format("Pass 2:  Parent page %d's " +
                            "left key is greater than inner page %d's first key",
                            parentPageNo, pageNo));
                    }
                }

                for (int k = 1; k < numKeys; k++) {
                    Tuple key = leaf.getKey(k);
                    int cmp = TupleComparator.compareTuples(prevKey, key);
                    if (cmp == 0) {
                        errors.add(String.format("Pass 2:  Leaf page %d keys " +
                            "%d and %d are duplicates!", pageNo, k - 1, k));
                    }
                    else if (cmp > 0) {
                        errors.add(String.format("Pass 2:  Leaf page %d keys " +
                            "%d and %d are out of order!", pageNo, k - 1, k));
                    }
                    prevKey = key;
                }

                if (parentRightKey != null) {
                    int cmp = TupleComparator.compareTuples(prevKey, parentRightKey);
                    // The parent's right-key should be greater than the last
                    // key in this page.
                    if (cmp >= 0) {
                        errors.add(String.format("Pass 2:  Parent page %d's " +
                            "right key is less than or equal to inner page " +
                            "%d's last key", parentPageNo, pageNo));
                    }
                }
            }

            break;
        }
        
        default:
            errors.add(String.format("Pass 2:  Can reach page %d from root, " +
                "but it's  not a leaf or an inner page!  Type = %d", pageNo,
                info.pageType));
        }
    }


    private void pass3ScanThruLeafList() throws IOException {
        logger.debug("Pass 3:  Scan through leaf page list");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
        int pageNo = HeaderPage.getRootPageNo(dbpHeader);

        // Walk down the leftmost pointers in the inner pages until we reach
        // the leftmost leaf page.  Then we can walk across the leaves and
        // check the constraints that should hold on leaves.
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        int pageType = dbPage.readUnsignedByte(0);
        while (pageType != BTreeIndexManager.BTREE_LEAF_PAGE) {
            if (pageType != BTreeIndexManager.BTREE_INNER_PAGE) {
                errors.add(String.format("Pass 3:  Page %d should be an inner " +
                    "page, but its type is %d instead", pageNo, pageType));
            }
            
            InnerPage innerPage = new InnerPage(dbPage, idxFileInfo);
            pageNo = innerPage.getPointer(0);
            dbPage = storageManager.loadDBPage(dbFile, pageNo);
            pageType = dbPage.readUnsignedByte(0);
        }

        // Now we should be at the leftmost leaf in the sequence of leaves.
        Tuple prevKey = null;
        int prevKeyPageNo = 0;
        while (true) {
            PageInfo info = pages.get(pageNo);
            info.numLeafListReferences++;

            LeafPage leafPage = new LeafPage(dbPage, idxFileInfo);

            for (int k = 0; k < leafPage.getNumEntries(); k++) {
                Tuple key = leafPage.getKey(k);
                
                if (prevKey != null) {
                    int cmp = TupleComparator.compareTuples(prevKey, key);
                    if (cmp == 0) {
                        if (prevKeyPageNo == pageNo) {
                            errors.add(String.format("Pass 3:  Leaf page %d " +
                                "keys %d and %d are duplicates!", pageNo,
                                k - 1, k));
                        }
                        else {
                            errors.add(String.format("Pass 3:  Leaf page %d " +
                                "key 0 is a duplicate to previous leaf %d's " +
                                "last key!", pageNo, prevKeyPageNo));
                        }
                    }
                    else if (cmp > 0) {
                        if (prevKeyPageNo == pageNo) {
                            errors.add(String.format("Pass 3:  Leaf page %d " +
                                "keys %d and %d are out of order!", pageNo,
                                k - 1, k));
                        }
                        else {
                            errors.add(String.format("Pass 3:  Leaf page %d " +
                                "key 0 is out of order with previous leaf %d's " +
                                "last key!", pageNo, prevKeyPageNo));
                        }
                    }
                }

                prevKey = key;
                prevKeyPageNo = pageNo;
            }
            
            // Go to the next leaf in the sequence.

            pageNo = leafPage.getNextPageNo();
            if (pageNo == 0)
                break;

            dbPage = storageManager.loadDBPage(dbFile, pageNo);
            pageType = dbPage.readUnsignedByte(0);

            if (pageType != BTreeIndexManager.BTREE_LEAF_PAGE) {
                errors.add(String.format("Pass 3:  Page %d should be a leaf " +
                    "page, but its type is %d instead", pageNo, pageType));
            }
        }
    }


    private void pass4ScanThruEmptyList() throws IOException {
        logger.debug("Pass 4:  Scan through empty page list");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
        int emptyPageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);

        while (emptyPageNo != 0) {
            PageInfo info = pages.get(emptyPageNo);
            info.numEmptyListReferences++;

            if (info.pageType != BTreeIndexManager.BTREE_EMPTY_PAGE) {
                errors.add(String.format("Page %d is in the empty-page list, " +
                    "but it isn't an empty page!  Type = %d", emptyPageNo,
                    info.pageType));
            }

            DBPage dbPage = storageManager.loadDBPage(dbFile, emptyPageNo);
            emptyPageNo = dbPage.readUnsignedShort(1);
        }
    }
    
    
    private void pass5ExaminePageReachability() {
        logger.debug("Pass 5:  Find pages with reachability issues");

        for (int pageNo = 1; pageNo < pages.size(); pageNo++) {
            PageInfo info = pages.get(pageNo);

            if (info.pageType == BTreeIndexManager.BTREE_INNER_PAGE ||
                info.pageType == BTreeIndexManager.BTREE_LEAF_PAGE) {
                if (info.numTreeReferences != 1) {
                    errors.add(String.format("Index page %d should have " +
                        "exactly one tree-reference, but has %d instead",
                        info.pageNo, info.numTreeReferences));
                }
                
                if (info.pageType == BTreeIndexManager.BTREE_LEAF_PAGE &&
                    info.numLeafListReferences != 1) {
                    errors.add(String.format("Leaf page %d should have " +
                        "exactly one leaf-list reference, but has %d instead",
                        info.pageNo, info.numLeafListReferences));
                }
            }
            else if (info.pageType == BTreeIndexManager.BTREE_EMPTY_PAGE) {
                if (info.numEmptyListReferences != 1) {
                    errors.add(String.format("Empty page %d should have " +
                        "exactly one empty-list reference, but has %d instead",
                        info.pageNo, info.numEmptyListReferences));
                }
            }
        }
    }
}
