package edu.caltech.nanodb.storage.btreeindex;


import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;


/**
 * This class provides high-level B<sup>+</sup> tree management operations
 * performed on leaf nodes.  These operations are provided here and not on the
 * {@link LeafPage} class since they sometimes involve splitting or merging
 * leaf nodes, updating parent nodes, and so forth.
 */
public class LeafPageOperations {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(LeafPageOperations.class);

    private StorageManager storageManager;

    private BTreeIndexManager bTreeManager;

    private InnerPageOperations innerPageOps;


    public LeafPageOperations(BTreeIndexManager bTreeManager,
                              InnerPageOperations innerPageOps) {
        this.bTreeManager = bTreeManager;
        this.storageManager = StorageManager.getInstance();
        this.innerPageOps = innerPageOps;
    }


    /**
     * This helper function provides the simple operation of loading a leaf page
     * from its page-number, or if the page-number is 0 then {@code null} is
     * returned.
     *
     * @param idxFileInfo the details of the index to load the leaf-page for.
     * @param pageNo the page-number to load as a leaf-page.
     *
     * @return a newly initialized {@link LeafPage} instance if {@code pageNo}
     *         is positive, or {@code null} if {@code pageNo} is 0.
     *
     * @throws IOException if an IO error occurs while loading the specified
     *         page
     *         
     * @throws IllegalArgumentException if the specified page isn't a leaf-page
     */
    private LeafPage loadLeafPage(IndexFileInfo idxFileInfo, int pageNo)
        throws IOException {

        if (pageNo == 0)
            return null;

        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, pageNo);
        return new LeafPage(dbPage, idxFileInfo);
    }


    /**
     * This helper function handles the operation of adding a new index-key and
     * tuple-pointer (contained within the key) to a leaf-page of the index.
     * This operation is provided here and not on the {@link LeafPage} class,
     * because adding the new entry might require the leaf page to be split into
     * two pages.
     *
     * @param leaf the leaf page to add the entry to
     *
     * @param newTupleKey the new entry to add to the leaf page
     *
     * @param pagePath the path of pages taken from the root page to this leaf
     *        page, represented as a list of page numbers in the data file
     *
     * @throws IOException if an IO error occurs while updating the index
     */
    public void addEntry(LeafPage leaf, TupleLiteral newTupleKey,
                         List<Integer> pagePath) throws IOException {

        // Figure out where the new key-value goes in the leaf page.

        int newEntrySize = newTupleKey.getStorageSize();
        if (leaf.getFreeSpace() < newEntrySize) {
            // Try to relocate entries from this leaf to either sibling,
            // or if that can't happen, split the leaf page into two.
            if (!relocateEntriesAndAddKey(leaf, pagePath, newTupleKey))
                splitLeafAndAddKey(leaf, pagePath, newTupleKey);
        }
        else {
            // There is room in the leaf for the new key.  Add it there.
            leaf.addEntry(newTupleKey);
        }
    }



    private boolean relocateEntriesAndAddKey(LeafPage page,
        List<Integer> pagePath, TupleLiteral key) throws IOException {

        // See if we are able to relocate records either direction to free up
        // space for the new key.

        int bytesRequired = key.getStorageSize();

        IndexFileInfo idxFileInfo = page.getIndexFileInfo();

        int pathSize = pagePath.size();
        if (pathSize == 1)  // This node is also the root - no parent.
            return false;   // There aren't any siblings to relocate to.
        
        if (pagePath.get(pathSize - 1) != page.getPageNo()) {
            throw new IllegalArgumentException(
                "leaf page number doesn't match last page-number in page path");
        }

        int parentPageNo = 0;
        if (pathSize >= 2)
            parentPageNo = pagePath.get(pathSize - 2);

        InnerPage parentPage = innerPageOps.loadPage(idxFileInfo, parentPageNo);
        int numPointers = parentPage.getNumPointers();
        int pagePtrIndex = parentPage.getIndexOfPointer(page.getPageNo());

        // Check each sibling in its own code block so that we can constrain
        // the scopes of the variables a bit.  This keeps us from accidentally
        // reusing the "prev" variables in the "next" section.

        {
            LeafPage prevPage = null;
            if (pagePtrIndex - 1 >= 0) {
                prevPage = loadLeafPage(idxFileInfo,
                    parentPage.getPointer(pagePtrIndex - 1));
            }

            if (prevPage != null) {
                // See if we can move some of this leaf's entries to the
                // previous leaf, to free up space.

                int count = tryLeafRelocateForSpace(page, prevPage, bytesRequired);
                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d entries from " +
                        "leaf-page %d to left-sibling leaf-page %d", count,
                        page.getPageNo(), prevPage.getPageNo()));

                    logger.debug("Space before relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    page.moveEntriesLeft(prevPage, count);

                    logger.debug("Space after relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        prevPage.getFreeSpace() + " bytes");

                    BTreeIndexPageTuple firstRightKey =
                        addEntryToLeafPair(prevPage, page, key);

                    pagePath.remove(pathSize - 1);
                    innerPageOps.replaceKey(parentPage, pagePath,
                        prevPage.getPageNo(), firstRightKey, page.getPageNo());

                    return true;
                }
            }
        }

        {
            LeafPage nextPage = null;
            if (pagePtrIndex + 1 < numPointers) {
                nextPage = loadLeafPage(idxFileInfo,
                    parentPage.getPointer(pagePtrIndex + 1));
            }

            if (nextPage != null) {
                // See if we can move some of this leaf's entries to the next
                // leaf, to free up space.

                int count = tryLeafRelocateForSpace(page, nextPage, bytesRequired);
                if (count > 0) {
                    // Yes, we can do it!

                    logger.debug(String.format("Relocating %d entries from " +
                        "leaf-page %d to right-sibling leaf-page %d", count,
                        page.getPageNo(), nextPage.getPageNo()));

                    logger.debug("Space before relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    page.moveEntriesRight(nextPage, count);

                    logger.debug("Space after relocation:  Leaf = " +
                        page.getFreeSpace() + " bytes\t\tSibling = " +
                        nextPage.getFreeSpace() + " bytes");

                    BTreeIndexPageTuple firstRightKey =
                        addEntryToLeafPair(page, nextPage, key);

                    pagePath.remove(pathSize - 1);
                    innerPageOps.replaceKey(parentPage, pagePath,
                        page.getPageNo(), firstRightKey, nextPage.getPageNo());

                    return true;
                }
            }
        }

        // Couldn't relocate entries to either the prevous or next page.  We
        // must split the leaf into two.
        return false;
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

        logger.debug("Leaf bytes free:  " + leafBytesFree +
            "\t\tAdjacent leaf bytes free:  " + adjBytesFree);

        // Subtract the bytes-required from the adjacent-bytes-free value so
        // that we ensure we always have room to put the key in either node.
        adjBytesFree -= bytesRequired;

        int numRelocated = 0;
        while (true) {
            int keySize = leaf.getKeySize(numRelocated);

            logger.debug("Key " + numRelocated + " is " + keySize + " bytes");

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

        logger.debug("Can relocate " + numRelocated + " keys to free up space.");

        return numRelocated;
    }


    private void splitLeafAndAddKey(LeafPage leaf, List<Integer> pagePath,
                                    TupleLiteral key) throws IOException {

        int pathSize = pagePath.size();
        if (pagePath.get(pathSize - 1) != leaf.getPageNo()) {
            throw new IllegalArgumentException(
                "Leaf page number doesn't match last page-number in page path");
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("Splitting leaf-page " + leaf.getPageNo() +
                " into two leaves.");
            logger.debug("    Old next-page:  " + leaf.getNextPageNo());
        }

        // Get a new blank page in the index, with the same parent as the
        // leaf-page we were handed.

        IndexFileInfo idxFileInfo = leaf.getIndexFileInfo();
        DBFile dbFile = idxFileInfo.getDBFile();
        DBPage newDBPage = bTreeManager.getNewDataPage(dbFile);
        LeafPage newLeaf = LeafPage.init(newDBPage, idxFileInfo);

        // Chain the leaf-page into the sequence with the previous and next
        // leaves.  The new leaf always follows the leaf we were handed.
        newLeaf.setNextPageNo(leaf.getNextPageNo());
        leaf.setNextPageNo(newLeaf.getPageNo());

        if (logger.isDebugEnabled()) {
            logger.debug("    New next-page:  " + leaf.getNextPageNo());
            logger.debug("    New next-leaf next-page:  " + newLeaf.getNextPageNo());
        }

        // Figure out how many values we want to move from the old page to the
        // new page.

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

        // If the current node doesn't have a parent, it's because it's
        // currently the root.
        if (pathSize == 1) {
            // Create a new root node and set both leaves to have it as their
            // parent.
            DBPage parentPage = bTreeManager.getNewDataPage(dbFile);
            InnerPage.init(parentPage, idxFileInfo,
                leaf.getPageNo(), firstRightKey, newLeaf.getPageNo());

            int parentPageNo = parentPage.getPageNo();

            // We have a new root-page in the index!
            DBPage dbpHeader = storageManager.loadDBPage(dbFile, 0);
            HeaderPage.setRootPageNo(dbpHeader, parentPageNo);

            logger.debug("Set index root-page to inner-page " + parentPageNo);
        }
        else {
            // Add the new leaf into the parent non-leaf node.  (This may cause
            // the parent node's contents to be moved or split, if the parent
            // is full.)

            // (We already set the new leaf's parent-page-number earlier.)

            int parentPageNo = pagePath.get(pathSize - 2);
            DBPage dbpParent = storageManager.loadDBPage(dbFile, parentPageNo);
            InnerPage parentPage = new InnerPage(dbpParent, idxFileInfo);
            
            pagePath.remove(pathSize - 1);
            innerPageOps.addEntry(parentPage, pagePath,
                leaf.getPageNo(), firstRightKey, newLeaf.getPageNo());

            logger.debug("Parent page " + parentPageNo + " now has " +
                parentPage.getNumPointers() + " page-pointers.");
        }
    }

}
