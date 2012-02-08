package edu.caltech.nanodb.storage;


import java.io.IOException;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.relations.ColumnIndexes;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.EventDispatchException;
import edu.caltech.nanodb.server.RowEventListener;
import org.apache.log4j.Logger;


/**
 * This class implements the {@link RowEventListener} interface to make sure
 * that all indexes on an updated table are kept up-to-date.  This handler is
 * installed by the {@link StorageManager#init} setup method.
 */
public class IndexUpdater implements RowEventListener {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexUpdater.class);


    /**
     * A cached reference to the storage manager since we use it a lot in this
     * class.
     */
    private StorageManager storageManager;
    
    
    public IndexUpdater(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
    }
    

    @Override
    public void beforeRowInserted(TableFileInfo tblFileInfo, Tuple newValues) {
        // Ignore.
    }

    @Override
    public void afterRowInserted(TableFileInfo tblFileInfo, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }
        
        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowUpdated(TableFileInfo tblFileInfo, Tuple oldTuple,
                                 Tuple newValues) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowUpdated(TableFileInfo tblFileInfo, Tuple oldValues,
                                Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowDeleted(TableFileInfo tblFileInfo, Tuple oldTuple) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowDeleted(TableFileInfo tblFileInfo, Tuple oldValues) {
        // Ignore.
    }


    private void addRowToIndexes(TableFileInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Adding tuple to indexes for table " +
            tblFileInfo.getTableName());
        
        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnIndexes indexDef : schema.getIndexes().values()) {

            logger.debug("Adding tuple to index " + indexDef.getIndexName());

            try {
                IndexFileInfo idxFileInfo = storageManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                IndexManager indexManager = idxFileInfo.getIndexManager();
                indexManager.addTuple(idxFileInfo, schema, indexDef, ptup);
            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName());
            }
        }
    }

    
    private void removeRowFromIndexes(TableFileInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Removing tuple from indexes for table " +
            tblFileInfo.getTableName());

        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnIndexes indexDef : schema.getIndexes().values()) {
            logger.debug("Removing tuple from index " + indexDef.getIndexName());

            try {
                IndexFileInfo idxFileInfo = storageManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                IndexManager indexManager = idxFileInfo.getIndexManager();
                indexManager.deleteTuple(idxFileInfo, ptup);
            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName());
            }
        }
    }
}
