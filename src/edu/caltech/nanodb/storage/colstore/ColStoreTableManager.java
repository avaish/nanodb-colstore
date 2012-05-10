package edu.caltech.nanodb.storage.colstore;


import java.io.EOFException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.ColumnStatsCollector;
import edu.caltech.nanodb.qeval.TableStats;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.ForeignKeyColumnIndexes;
import edu.caltech.nanodb.relations.KeyColumnIndexes;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.BlockedTableReader;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileAnalyzer;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.heapfile.HeapFileTableManager;


/**
 * This class manages heap files that use the slotted page format for storing
 * variable-size tuples.
 */
public class ColStoreTableManager implements TableManager {

	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(ColStoreTableManager.class);
    
    public static final int COMP_OFFSET = 2;
    
    public static final int RLE_COMP = 1;

    
    /**
     * The table manager uses the storage manager a lot, so it caches a reference
     * to the singleton instance of the storage manager at initialization.
     */
    private StorageManager storageManager;
    
	
	public ColStoreTableManager(StorageManager storageManager) {
		if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.storageManager = storageManager;
	}

	@Override
	public void initTableInfo(TableFileInfo tblFileInfo) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void loadTableInfo(TableFileInfo tblFileInfo) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beforeCloseTable(TableFileInfo tblFileInfo) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void beforeDropTable(TableFileInfo tblFileInfo) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Tuple getFirstTuple(TableFileInfo tblFileInfo) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple getNextTuple(TableFileInfo tblFileInfo, Tuple tup)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple getTuple(TableFileInfo tblFileInfo, FilePointer fptr)
			throws InvalidFilePointerException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Tuple addTuple(TableFileInfo tblFileInfo, Tuple tup)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateTuple(TableFileInfo tblFileInfo, Tuple tup,
			Map<String, Object> newValues) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteTuple(TableFileInfo tblFileInfo, Tuple tup)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void analyzeTable(TableFileInfo tblFileInfo) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BlockedTableReader getBlockedReader() {
		// TODO Auto-generated method stub
		return null;
	}

	public void writeTable(FileAnalyzer analyzer, TableFileInfo tblFileInfo) {
		for (int i = 0; i < tblFileInfo.getSchema().numColumns(); i++)
		{
			// Get the column's DBFile
			DBFile dbFile = tblFileInfo.getDBFile(i + 1);
			logger.debug("Filling " + dbFile);
		}
	}
	
}
