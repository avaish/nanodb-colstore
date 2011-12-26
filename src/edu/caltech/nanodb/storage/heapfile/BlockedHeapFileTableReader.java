package edu.caltech.nanodb.storage.heapfile;


import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.BlockedTableReader;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;

import java.io.EOFException;
import java.io.IOException;


/**
 * An implementation of the <tt>BlockedTableReader</tt> interface, allowing
 * heap table-files to be read/scanned in a block-by-block manner, as opposed to
 * a tuple-by-tuple manner.
 */
public class BlockedHeapFileTableReader implements BlockedTableReader {
    /**
     * The table reader uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;


    /**
     * Initializes the blocked heap-file table reader.  All the constructor
     * currently does is to cache a reference to the singleton
     * {@link StorageManager}, since it is used so extensively.
     */
    public BlockedHeapFileTableReader() {
        this.storageManager = StorageManager.getInstance();
    }


    // Inherit Javadocs.
    @Override
    public DBPage getFirstDataPage(TableFileInfo tblFileInfo) throws IOException {
        // Try to fetch the first data page.  If none exists, return null.
        DBPage dbPage = null;
        try {
            dbPage = storageManager.loadDBPage(tblFileInfo.getDBFile(), 1);
        }
        catch (EOFException e) {
            // Ignore.
        }
        return dbPage;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getLastDataPage(TableFileInfo tblFileInfo) throws IOException {
        // Try to fetch the last data page.  If none exists, return null.
        DBFile dbFile = tblFileInfo.getDBFile();
        int numPages = dbFile.getNumPages();

        DBPage dbPage = null;
        // If we have at least 2 pages, then we have at least 1 data page!
        if (numPages >= 2)
            dbPage = storageManager.loadDBPage(dbFile, numPages - 1);

        return dbPage;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getNextDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException {

        DBFile dbFile = tblFileInfo.getDBFile();
        int numPages = dbFile.getNumPages();

        DBPage nextPage = null;
        int nextPageNo = dbPage.getPageNo() + 1;
        if (nextPageNo < numPages)
            nextPage = storageManager.loadDBPage(dbFile, nextPageNo);

        return nextPage;
    }


    // Inherit Javadocs.
    @Override
    public DBPage getPrevDataPage(TableFileInfo tblFileInfo, DBPage dbPage)
        throws IOException {

        DBFile dbFile = tblFileInfo.getDBFile();

        DBPage prevPage = null;
        int prevPageNo = dbPage.getPageNo() - 1;
        if (prevPageNo >= 1)
            prevPage = storageManager.loadDBPage(dbFile, prevPageNo);

        return prevPage;
    }


    // Inherit Javadocs.
    @Override
    public Tuple getFirstTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage) {
        int slot = 0;
        int numSlots = DataPage.getNumSlots(dbPage);
        while (slot < numSlots) {
            int nextOffset = DataPage.getSlotValue(dbPage, slot);
            if (nextOffset != DataPage.EMPTY_SLOT)
                return new HeapFilePageTuple(tblFileInfo, dbPage, slot, nextOffset);

            slot++;
        }
        return null;
    }


    // Inherit Javadocs.
    @Override
    public Tuple getNextTupleInPage(TableFileInfo tblFileInfo, DBPage dbPage,
        Tuple tup) {

        if (!(tup instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type PageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        int nextSlot = ptup.getSlot() + 1;
        int numSlots = DataPage.getNumSlots(dbPage);
        while (nextSlot < numSlots) {
            int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
            if (nextOffset != DataPage.EMPTY_SLOT) {
                return new HeapFilePageTuple(tblFileInfo, dbPage, nextSlot,
                    nextOffset);
            }

            nextSlot++;
        }
        return null;
    }
}
