package edu.caltech.nanodb.storage.btreeindex;


import java.io.IOException;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.StorageManager;
import org.apache.log4j.Logger;


/**
 * Created by IntelliJ IDEA.
 * User: donnie
 * Date: 12/19/11
 * Time: 10:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class BTreeIndexManager implements IndexManager {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(BTreeIndexManager.class);


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
        logger.info("Writing index specification");

/***
        hpWriter.writeVarString255(idxFileInfo.getTableName());
        for (ColumnInfo colInfo : idxFileInfo.getColumnInfos()) {
            ColumnType colType = colInfo.getType();

                // Each column description consists of a type specification, a set
                // of flags (1 byte), and a string specifying the column's name.

                // Write the SQL data type and any associated details.

                hpWriter.writeByte(colType.getBaseType().getTypeID());

                // If this data type requires additional details, write that as well.
                if (colType.hasLength()) {
                    // CHAR and VARCHAR fields have a 2 byte length value after the type.
                    hpWriter.writeShort(colType.getLength());
                }

                // Write the column name.
                hpWriter.writeVarString255(colInfo.getName());
            }

            // Compute and store the schema's size.
            int schemaSize = hpWriter.getPosition() - HeaderPage.OFFSET_NCOLS;
            HeaderPage.setSchemaSize(headerPage, schemaSize);

            // Report how much space was used by schema info.  (It's the current
            // position minus 4 bytes, since the first 2 bytes are file-type and
            // encoded page size, and the second 2 bytes are the schema size.)
            logger.debug("Table " + tableName + " schema uses " + schemaSize +
                " bytes of the " + dbFile.getPageSize() + "-byte header page.");
        }
***/
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
}
