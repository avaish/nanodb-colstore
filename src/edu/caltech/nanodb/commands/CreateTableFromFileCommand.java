package edu.caltech.nanodb.commands;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import edu.caltech.nanodb.indexes.IndexInfo;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.indexes.IndexFileInfo;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ForeignKeyColumnIndexes;
import edu.caltech.nanodb.relations.KeyColumnIndexes;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;

import edu.caltech.nanodb.storage.DBFileType;
import edu.caltech.nanodb.storage.FileAnalyzer;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.colstore.ColStoreTableManager;


/**
 * This command handles the <tt>CREATE COLSTORE</tt> DDL operation.
 */
public class CreateTableFromFileCommand extends CreateTableCommand {
	
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CreateTableCommand.class);
    
    /** Name of the backing file from which table is being created. */
    private String fileName;

    
    public CreateTableFromFileCommand(String tableName, String fileName) {
    	super(tableName, false, false);
    	
    	if (fileName == null)
    		throw new IllegalArgumentException("fileName cannot be null");

    	this.fileName = fileName;
    	logger.debug("Creating " + super.getTableName() + " from " + fileName);
    }


	@Override
	public void execute() throws ExecutionException {
        StorageManager storageManager = StorageManager.getInstance();


        // Set up the table-file info based on the command details.

        logger.debug("Creating a TableFileInfo object describing the new table " +
            getTableName() + ".");
        TableFileInfo tblFileInfo = new TableFileInfo(getTableName());
        tblFileInfo.setFileType(DBFileType.HEAP_DATA_FILE);
        TableSchema schema = tblFileInfo.getSchema();
        logger.debug(schema);
        for (ColumnInfo colInfo : getColumnInfos()) {
            try {
                schema.addColumnInfo(colInfo);
            }
            catch (IllegalArgumentException iae) {
                throw new ExecutionException("Duplicate or invalid column \"" +
                    colInfo.getName() + "\".", iae);
            }
        }
        
        logger.debug(schema);

        // Open all tables referenced by foreign-key constraints, so that we can
        // verify the constraints.
        HashMap<String, TableSchema> referencedTables =
            new HashMap<String, TableSchema>();
        for (ConstraintDecl cd: getConstraints()) {
            if (cd.getType() == TableConstraintType.FOREIGN_KEY) {
                String refTableName = cd.getRefTable();
                try {
                    TableFileInfo refTblFileInfo =
                        storageManager.openTable(refTableName);
                    referencedTables.put(refTableName, refTblFileInfo.getSchema());
                }
                catch (FileNotFoundException e) {
                    throw new ExecutionException(String.format(
                        "Referenced table %s doesn't exist.", refTableName), e);
                }
                catch (IOException e) {
                    throw new ExecutionException(String.format(
                        "Error while loading schema for referenced table %s.",
                        refTableName), e);
                }
            }
        }

        try {
            initTableConstraints(storageManager, tblFileInfo, referencedTables);
        }
        catch (IOException e) {
            throw new ExecutionException(
                "Couldn't initialize all constraints on table " + getTableName(), e);
        }

        // Get the table manager and create the table.

        logger.debug("Creating the new table " + getTableName() + " on disk.");
        try {
            storageManager.createTable(tblFileInfo);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not create table \"" + getTableName() +
                "\".  See nested exception for details.", ioe);
        }
        logger.debug("New table " + getTableName() + " is created!");

        out.println("Created table:  " + getTableName());
        
        FileAnalyzer analyzer = null;
        try {
			analyzer = new FileAnalyzer(fileName);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        analyzer.generateTuples(tblFileInfo);
        
        
    }
}