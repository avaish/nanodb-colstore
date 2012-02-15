package edu.caltech.nanodb.storage.btreeindex;

import edu.caltech.nanodb.expressions.TupleComparator;
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
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 2/15/12
 * Time: 1:13 PM
 * To change this template use File | Settings | File Templates.
 */
public class BTreeIndexVerifier {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BTreeIndexVerifier.class);

    
    private static class PageInfo {
        /** The page's number. */
        public int pageNo;

        /** The type of the index page. */
        public int pageType;

        /** A flag indicating whether the page is accessible from the root. */
        public boolean accessibleFromRoot;

        /**
         * Records how many times the page has been referenced in the index
         * structure.
         */
        public int numReferences;
        
        public PageInfo(int pageNo, int pageType) {
            this.pageNo = pageNo;
            this.pageType = pageType;

            accessibleFromRoot = false;
            numReferences = 0;
        }
    }
    
    
    private StorageManager storageManager;
    
    
    private IndexFileInfo idxFileInfo;

    
    private DBFile dbFile;
    

    private HashMap<Integer, PageInfo> pages;
    
    
    private int numEmptyPages;
    
    
    private ArrayList<String> errors;

    
    
    public BTreeIndexVerifier(IndexFileInfo idxFileInfo) {
        storageManager = StorageManager.getInstance();
        this.idxFileInfo = idxFileInfo;
        dbFile = idxFileInfo.getDBFile();
    }
    
    
    public List<String> verify() throws IOException {
        errors = new ArrayList<String>();
        numEmptyPages = 0;
        
        doPass1();
        doPass2();
        doPass3();
        doPass4();

        return errors;
    }
    
    
    private void doPass1() throws IOException {
        logger.debug("Pass 1:  Linear scan through pages to collect info");

        pages = new HashMap<Integer, PageInfo>();
        for (int pageNo = 1; pageNo < dbFile.getNumPages(); pageNo++) {
            DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);

            int pageType = dbPage.readUnsignedByte(0);
            PageInfo info = new PageInfo(pageNo, pageType);
            pages.put(pageNo, info);
        }
    }
    
    
    private void doPass2() throws IOException {
        logger.debug("Pass 2:  Tree scan from root to verify nodes");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
        int rootPageNo = HeaderPage.getRootPageNo(dbpHeader);
        scanTree(rootPageNo);
    }
    
    
    private void scanTree(int pageNo) throws IOException {
        PageInfo info = pages.get(pageNo);
        info.accessibleFromRoot = true;
        info.numReferences++;

        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        
        switch (info.pageType) {
        case BTreeIndexManager.BTREE_INNER_PAGE:
        {
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
                    errors.add(String.format(
                        "Inner page %d references both inner and leaf pages.",
                        pageNo));
                }
                
                if (refOther != 0) {
                    errors.add(String.format("Inner page %d references pages " +
                        "that are neither inner pages nor leaf pages.", pageNo));
                }
            }
            
            // Make sure the keys are in the proper order in the page.
            
            int numKeys = inner.getNumKeys();
            if (numKeys > 1) {
                Tuple prevKey = inner.getKey(0);
                for (int k = 1; k < numKeys; k++) {
                    Tuple key = inner.getKey(k);
                    int cmp = TupleComparator.compareTuples(prevKey, key);
                    if (cmp == 0) {
                        errors.add(String.format(
                            "Inner page %d keys %d and %d are duplicates!",
                            pageNo, k - 1, k));
                    }
                    else if (cmp > 0) {
                        errors.add(String.format(
                            "Inner page %d keys %d and %d are out of order!",
                            pageNo, k - 1, k));
                    }
                }
            }
            
            // Now that we are done with this page, check each child-page.

            for (int refPageNo : refPages)
                scanTree(refPageNo);

            break;
        }

        case BTreeIndexManager.BTREE_LEAF_PAGE:
        {
            LeafPage leaf = new LeafPage(dbPage, idxFileInfo);

            // Make sure the keys are in the proper order in the page.

            int numKeys = leaf.getNumEntries();
            if (numKeys > 1) {
                Tuple prevKey = leaf.getKey(0);
                for (int k = 1; k < numKeys; k++) {
                    Tuple key = leaf.getKey(k);
                    int cmp = TupleComparator.compareTuples(prevKey, key);
                    if (cmp == 0) {
                        errors.add(String.format(
                            "Leaf page %d keys %d and %d are duplicates!",
                            pageNo, k - 1, k));
                    }
                    else if (cmp > 0) {
                        errors.add(String.format(
                            "Leaf page %d keys %d and %d are out of order!",
                            pageNo, k - 1, k));
                    }
                }
            }

            break;
        }
        
        default:
            errors.add(String.format("Can reach page %d from root, but it's " +
                "not a leaf or an inner page!  Type = %d", pageNo, info.pageType));
        }
    }


    private void doPass3() throws IOException {
        logger.debug("Pass 3:  Scan through empty page list");

        DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
        int emptyPageNo = HeaderPage.getFirstEmptyPageNo(dbpHeader);

        while (emptyPageNo != 0) {
            numEmptyPages++;
            PageInfo info = pages.get(emptyPageNo);
            info.numReferences++;
            
            if (info.pageType != BTreeIndexManager.BTREE_EMPTY_PAGE) {
                errors.add(String.format("Page %d is in the empty-page list, " +
                    "but it isn't an empty page!  Type = %d", emptyPageNo,
                    info.pageType));
            }

            DBPage dbPage = storageManager.loadDBPage(dbFile, emptyPageNo);
            emptyPageNo = dbPage.readUnsignedShort(1);
        }
    }
    
    
    private void doPass4() {
        logger.debug("Pass 4:  Identify pages with reachability issues");
        
        for (int pageNo = 1; pageNo < pages.size(); pageNo++) {
            PageInfo info = pages.get(pageNo);
            
            if (info.numReferences != 1) {
                errors.add(String.format("Orphan page %d is not reachable " +
                    "from either the root node or the empty-page list",
                    info.pageNo));
            }
        }
    }
}
