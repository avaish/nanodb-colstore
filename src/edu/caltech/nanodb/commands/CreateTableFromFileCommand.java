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
		super.execute();
	}
}