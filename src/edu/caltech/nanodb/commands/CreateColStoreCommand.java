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

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * This command handles the <tt>CREATE COLSTORE</tt> DDL operation.
 */
public class CreateColStoreCommand extends Command {
	
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CreateTableCommand.class);


    /** Name of the table to be created. */
    private String tableName;
    
    
    /** Name of the backing file from which table is being created. */
    private String fileName;
    
    
    /** List of column-declarations for the new table. */
    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();

    
    public CreateColStoreCommand(String tableName, String fileName) {
    	super(Command.Type.DDL);

    	if (tableName == null)
    		throw new IllegalArgumentException("tableName cannot be null");
    	
    	if (fileName == null)
    		throw new IllegalArgumentException("fileName cannot be null");

    	this.tableName = tableName;
    	this.fileName = fileName;
    	logger.debug("Creating " + tableName + " from " + fileName);
    }


	@Override
	public void execute() throws ExecutionException {
		// Analyze file
		// Tell storageManager to make colstore table
	}
}