package edu.caltech.nanodb.storage.colstore;


import java.io.EOFException;
import java.io.IOException;

import java.util.*;

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

	public void writeTable(FileAnalyzer analyzer, TableFileInfo tblFileInfo) 
			throws IOException, InterruptedException {
		for (int i = 0; i < tblFileInfo.getSchema().numColumns(); i++)
		{
			// Get the column's DBFile and ColInfo
			DBFile dbFile = tblFileInfo.getDBFile(i + 1);
			ColumnInfo colInfo = tblFileInfo.getSchema().getColumnInfo(i);
			switch (analyzer.getEncoding(i)) {
			case RLE:
				writeRLE(dbFile, analyzer, i, colInfo);
				break;
			case DICTIONARY:
				writeDictionary(dbFile, analyzer, i, colInfo);
				break;
			case NONE:
				writeUncompressed(dbFile, analyzer, i, colInfo);
				break;
			}
		}
	}
	
	private void writeDictionary(DBFile file, FileAnalyzer analyzer, int index,
			ColumnInfo info) throws IOException {
		
		DBPage dbPage = storageManager.loadDBPage(file, 0);
		DictionaryPage.initNewPage(dbPage);
		
		dbPage = storageManager.loadDBPage(file, 1, true);
		DictionaryPage.initNewPage(dbPage);
		
		HashMap<String, Integer> dict = new HashMap<String, Integer>();
		
		int distincts = analyzer.getCounts(index);
		int bitsize = (int) Math.ceil(Math.log(distincts)/Math.log(2));
		
		logger.debug("Bitsize " + bitsize);
		
		int blockNum = (int) Math.floor(16.0 / bitsize);
		
		int val = 0;
		int currentBlock = 0;
		int blockIndex = 0;
		
		String object = analyzer.getNextObject(index);
		
		while (object != null) {
			if (!dict.containsKey(object)) {
				dict.put(object, val);
				val++;
			}
			
			int bitrep = dict.get(object);
			logger.debug(object + " bitrep: " + Integer.toBinaryString(bitrep));
			
			currentBlock = currentBlock | (bitrep << blockIndex);
			logger.debug("Current block: " + Integer.toBinaryString(currentBlock));
			
			logger.debug("");
			
			blockIndex++;
			if (blockIndex == blockNum) {
				logger.debug("Writing block: " + Integer.toBinaryString(currentBlock));
				logger.debug((short) currentBlock);

				if (DictionaryPage.writeBlock(dbPage, currentBlock, blockIndex)) {
					logger.debug("Written to file!");
				}
				else
				{
					dbPage = storageManager.loadDBPage(file, dbPage.getPageNo() + 1, true);
					DictionaryPage.initNewPage(dbPage);
					DictionaryPage.writeBlock(dbPage, currentBlock, blockIndex);
					logger.debug("New page loaded!");
				}
				
				blockIndex = 0;
				currentBlock = 0;
			}
			
			object = analyzer.getNextObject(index);
		}
		
		logger.debug("Writing block: " + Integer.toBinaryString(currentBlock));
		logger.debug((short) currentBlock);
		if (DictionaryPage.writeBlock(dbPage, currentBlock, blockIndex)) {
			logger.debug("Written to file!");
		}
		else
		{
			dbPage = storageManager.loadDBPage(file, dbPage.getPageNo() + 1, true);
			DictionaryPage.initNewPage(dbPage);
			DictionaryPage.writeBlock(dbPage, currentBlock, blockIndex);
			logger.debug("New page loaded!");
		}
		blockIndex = 0;
		currentBlock = 0;
		
		dbPage = storageManager.loadDBPage(file, 0);
		
		DictionaryPage.writeDictionary(dbPage, dict, bitsize, blockNum, info);
	}

	private void writeUncompressed(DBFile file, FileAnalyzer analyzer, int index,
			ColumnInfo info) throws IOException, InterruptedException {
		
		DBPage dbPage = storageManager.loadDBPage(file, 0);
		UncompressedPage.initNewPage(dbPage);
		
		int count = 0;
		
		String object = analyzer.getNextObject(index);
		
		while (object != null) {
		
			logger.debug("Entry: " + object);
			
			if (UncompressedPage.writeBlock(dbPage, object, count, info.getType())) {
				logger.debug("Written to file!");
			}
			else
			{
				dbPage = storageManager.loadDBPage(file, dbPage.getPageNo() + 1, true);
				UncompressedPage.initNewPage(dbPage);
				UncompressedPage.writeBlock(dbPage, object, count, info.getType());
				logger.debug("New page loaded!");
			}
			
			count++;
			object = analyzer.getNextObject(index);
		}
		
	}

	private void writeRLE(DBFile file, FileAnalyzer analyzer, int index, 
			ColumnInfo info) throws IOException, InterruptedException {
		
		DBPage dbPage = storageManager.loadDBPage(file, 0);
		RLEPage.initNewPage(dbPage);
		
		int position = 0;
		// Start creating RLE block
		
		String object = analyzer.getNextObject(index);
		
		while (object != null) {
			int start = position;
			int count = 1;
			
			String compare = analyzer.getNextObject(index);
			
			while (compare != null && compare.equals(object)) {
				count++;
				position++;
				compare = analyzer.getNextObject(index);
			}
			
			logger.debug("Run: (" + object + ", " + start + ", " + count + ")");
			
			if (RLEPage.writeBlock(dbPage, object, start, count, info.getType())) {
				logger.debug("Written to file!");
			}
			else
			{
				dbPage = storageManager.loadDBPage(file, dbPage.getPageNo() + 1, true);
				RLEPage.initNewPage(dbPage);
				RLEPage.writeBlock(dbPage, object, start, count, info.getType());
				logger.debug("New page loaded!");
			}
			
			analyzer.reset(index);
			position++;
			object = analyzer.getNextObject(index);
		}
	}
}
