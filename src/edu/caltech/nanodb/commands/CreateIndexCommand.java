package edu.caltech.nanodb.commands;

import edu.caltech.nanodb.indexes.IndexFileInfo;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * This command-class represents the <tt>CREATE INDEX</tt> DDL command.
 */
public class CreateIndexCommand extends Command {
    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(CreateIndexCommand.class);


    private String indexName;

    private String indexType;

    /**
     * This flag specifies whether the index is a unique index or not.  If the
     * value is true then no key-value may appear multiple times; if the value
     * is false then a key-value may appear multiple times.
     */
    private boolean unique;


    /** The name of the table that the index is built against. */
    private String tableName;


    /**
     * The list of column-names that the index is built against.  The order of
     * these values is important; for ordered indexes, the index records must be
     * kept in the order specified by the sequence of column names.
     */
    private ArrayList<String> columnNames = new ArrayList<String>();



    public CreateIndexCommand(String indexName, String indexType) {
        super(Type.DDL);

        if (indexName == null)
            throw new IllegalArgumentException("indexName cannot be null");

        if (indexType == null)
            throw new IllegalArgumentException("indexType cannot be null");

        this.indexName = indexName;
        this.indexType = indexType;
    }


    public void setTable(String tableName) {
        this.tableName = tableName;
    }


    public void setUnique(boolean unique) {
        this.unique = unique;
    }


    public boolean isUnique() {
        return unique;
    }


    public void addColumn(String columnName) {
        this.columnNames.add(columnName);
    }


    public void execute() throws ExecutionException {
        StorageManager storageManager = StorageManager.getInstance();
        // Set up the index-file info based on the command details.

        // Open the table and get the schema for the table.
        logger.debug(String.format("Opening table %s to retrieve schema",
            tableName));
        TableFileInfo tblFileInfo;
        try {
            tblFileInfo = storageManager.openTable(tableName);
        }
        catch (FileNotFoundException e) {
            throw new ExecutionException(String.format(
                "Specified table %s doesn't exist!", tableName), e);
        }
        catch (IOException e) {
            throw new ExecutionException(String.format(
                "Error occurred while opening table %s", tableName), e);
        }

        // Look up each column mentioned in the index.
        Schema schema = tblFileInfo.getSchema();
        ArrayList<ColumnInfo> colInfos = new ArrayList<ColumnInfo>();
        HashSet<String> colNameSet = new HashSet<String>();
        for (String colName : columnNames) {
            // Make sure we haven't already seen this column!
            if (colNameSet.contains(colName)) {
                throw new ExecutionException(String.format(
                    "Column %s was specified multiple times!", colName));
            }

            try {
                ColumnInfo colInfo = schema.getColumnInfo(colName);
                colInfos.add(colInfo);
            }
            catch (SchemaNameException e) {
                throw new ExecutionException(
                    String.format("Column %s doesn't exist", colName), e);
            }

            colNameSet.add(colName);
        }

        logger.debug(String.format("Creating an IndexFileInfo object " +
            "describing the new index %s on table %s.", indexName, tableName));

        IndexFileInfo idxFileInfo =
            new IndexFileInfo(indexName, tableName, null);

        // Get the index manager and create the index.

        logger.debug("Creating the new index " + indexName + " on disk.");
        try {
            StorageManager.getInstance().createIndex(idxFileInfo);
        }
        catch (IOException ioe) {
            throw new ExecutionException(String.format(
                "Could not create index \"%s\" on table \"%s\".  See nested " +
                "exception for details.", indexName, tableName), ioe);
        }
        logger.debug(String.format("New index %s on table %s is created!",
            indexName, tableName));

        System.out.println("Created index:  " + indexName);
    }
}
