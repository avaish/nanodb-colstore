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
import edu.caltech.nanodb.storage.DBFileType;
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
import edu.caltech.nanodb.storage.heapfile.HeaderPage;
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
		
		String tableName = tblFileInfo.getTableName();
        DBFile dbFile = tblFileInfo.getDBFile();
        TableSchema schema = tblFileInfo.getSchema();
        
        logger.info(String.format(
            "Initializing new column store %s with %d columns, stored at %s",
            tableName, schema.numColumns(), dbFile));
        
        DBPage dbPage = storageManager.loadDBPage(dbFile, 0);
        
        CSHeaderPage.initNewPage(dbPage);
        
        PageWriter hpWriter = new PageWriter(dbPage);
        hpWriter.setPosition(CSHeaderPage.SCHEMA_START_OFFSET);
        
        // Write out the schema details now.
        logger.info("Writing table schema:  " + schema);

        // Column details:
        hpWriter.writeByte(schema.numColumns());
        for (ColumnInfo colInfo : schema.getColumnInfos()) {

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

        // Write all details of key constraints, foreign keys, and indexes:

        int numConstraints = schema.numCandidateKeys() + schema.numForeignKeys();
        KeyColumnIndexes pk = schema.getPrimaryKey();
        if (pk != null)
            numConstraints++;

        logger.debug("Writing " + numConstraints + " constraints");
        int constraintStartIndex = hpWriter.getPosition();
        hpWriter.writeByte(numConstraints);

        if (pk != null)
            writeKey(hpWriter, TableConstraintType.PRIMARY_KEY, pk);

        for (KeyColumnIndexes ck : schema.getCandidateKeys())
            writeKey(hpWriter, TableConstraintType.UNIQUE, ck);

        for (ForeignKeyColumnIndexes fk : schema.getForeignKeys())
            writeForeignKey(hpWriter, fk);

        logger.debug("Constraints occupy " +
            (hpWriter.getPosition() - constraintStartIndex) +
            " bytes in the schema");
        
        // Compute and store the schema's size.
        int schemaSize = hpWriter.getPosition() - CSHeaderPage.SCHEMA_START_OFFSET;
        CSHeaderPage.setSchemaSize(dbPage, schemaSize);
        
        // Report how much space was used by schema info.  (It's the current
        // position minus 4 bytes, since the first 2 bytes are file-type and
        // encoded page size, and the second 2 bytes are the schema size.)
        logger.debug("Column store " + tableName + " schema uses " + schemaSize +
            " bytes of the " + dbFile.getPageSize() + "-byte header page.");
		
	}
	
	/**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     * 
     * @param hpWriter the writer being used to write the table's schema to its
     *        header page
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @param key a specification of what columns appear in the key
     *            
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    private void writeKey(PageWriter hpWriter, TableConstraintType type,
                          KeyColumnIndexes key) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            logger.debug(String.format(" * Primary key %s, enforced with index %s",
                key, key.getIndexName()));
        }
        else if (type == TableConstraintType.UNIQUE) {
            logger.debug(String.format(" * Candidate key %s, enforced with index %s",
                key, key.getIndexName()));
        }
        else {
            throw new IllegalArgumentException(
                "Invalid TableConstraintType value " + type);
        }

        int typeVal = type.getTypeID();
        String cName = key.getConstraintName();
        if (cName != null)
            typeVal |= 0x80;

        hpWriter.writeByte(typeVal);
        if (cName != null)
            hpWriter.writeVarString255(cName);

        hpWriter.writeByte(key.size());
        for (int i = 0; i < key.size(); i++)
            hpWriter.writeByte(key.getCol(i));

        // This should always be specified.
        hpWriter.writeVarString255(key.getIndexName());
    }
    
    
    private void writeForeignKey(PageWriter hpWriter, ForeignKeyColumnIndexes key) {
        logger.debug(" * Foreign key " + key);

        int type = TableConstraintType.FOREIGN_KEY.getTypeID();
        if (key.getConstraintName() != null)
            type |= 0x80;

        hpWriter.writeByte(type);
        if (key.getConstraintName() != null)
            hpWriter.writeVarString255(key.getConstraintName());

        hpWriter.writeVarString255(key.getRefTable());
        hpWriter.writeByte(key.size());
        for (int i = 0; i < key.size(); i++) {
            hpWriter.writeByte(key.getCol(i));
            hpWriter.writeByte(key.getRefCol(i));
        }
    }

	@Override
	public void loadTableInfo(TableFileInfo tblFileInfo) throws IOException {
		// Read in the table file's header page.  Wrap it in a page-reader to make
        // the input operations easier.

        String tableName = tblFileInfo.getTableName();
        DBFile dbFile = tblFileInfo.getDBFile();
        DBPage dbPage = storageManager.loadDBPage(dbFile, 0);
        PageReader hpReader = new PageReader(dbPage);
        
        hpReader.setPosition(CSHeaderPage.SCHEMA_START_OFFSET);
        
        // Read in the column descriptions.

        int numCols = hpReader.readUnsignedByte();
        logger.debug("Table has " + numCols + " columns.");

        if (numCols == 0)
            throw new IOException("Table must have at least one column.");

        TableSchema schema = tblFileInfo.getSchema();
        for (int iCol = 0; iCol < numCols; iCol++) {
            // Each column description consists of a type specification, a set
            // of flags (1 byte), and a string specifying the column's name.

            // Get the SQL data type, and begin to build the column's type
            // with that.

            byte sqlTypeID = hpReader.readByte();

            SQLDataType baseType = SQLDataType.findType(sqlTypeID);
            if (baseType == null) {
                throw new IOException("Unrecognized SQL type " + sqlTypeID +
                    " for column " + iCol);
            }

            ColumnType colType = new ColumnType(baseType);

            // If this data type requires additional details, read that as well.
            if (colType.hasLength()) {
                // CHAR and VARCHAR fields have a 2 byte length value after
                // the type.
                colType.setLength(hpReader.readUnsignedShort());
            }

            // TODO:  Read the column flags. (e.g. not-null, etc.)
            // int colFlags = hpReader.readUnsignedByte();

            // Read and verify the column name.

            String colName = hpReader.readVarString255();

            if (colName.length() == 0) {
                throw new IOException("Name of column " + iCol +
                    " is unspecified.");
            }

            for (int iCh = 0; iCh < colName.length(); iCh++) {
                char ch = colName.charAt(iCh);

                if (iCh == 0 && !(Character.isLetter(ch) || ch == '_') ||
                    iCh > 0 && !(Character.isLetterOrDigit(ch) || ch == '_')) {
                    throw new IOException(String.format("Name of column " +
                        "%d \"%s\" has an invalid character at index %d.",
                        iCol, colName, iCh));
                }
            }

            ColumnInfo colInfo = new ColumnInfo(colName, tableName, colType);
            
            logger.debug("Adding file " + tableName + "/" + tableName + "." + colName + ".tbl");
            tblFileInfo.addDBFile(storageManager.openDBFile(tableName + "/" + 
            	tableName + "." + colName + ".tbl"));

            logger.debug(colInfo);

            schema.addColumnInfo(colInfo);
        }

        // Read all details of key constraints, foreign keys, and indexes:

        int numConstraints = hpReader.readUnsignedByte();
        logger.debug("Reading " + numConstraints + " constraints");

        for (int i = 0; i < numConstraints; i++) {
            int cTypeID = hpReader.readUnsignedByte();
            TableConstraintType cType =
                TableConstraintType.findType((byte) (cTypeID & 0x7F));
            if (cType == null)
                throw new IOException("Unrecognized constraint-type value " + cTypeID);

            switch (cType) {
            case PRIMARY_KEY:
                schema.setPrimaryKey(
                    readKey(hpReader, cTypeID, TableConstraintType.PRIMARY_KEY));
                break;

            case UNIQUE:
                schema.addCandidateKey(
                    readKey(hpReader, cTypeID, TableConstraintType.UNIQUE));
                break;

            case FOREIGN_KEY:
                schema.addForeignKey(readForeignKey(hpReader, cTypeID));
                break;
            
            default:
                throw new IOException(
                    "Encountered unhandled constraint type " + cType);
            }
        }
        
        tblFileInfo.setFileType(DBFileType.COLUMNSTORE_DATA_FILE);
		
	}
	
	/**
     * This helper function writes a primary key or candidate key to the table's
     * schema stored in the header page.
     *
     * @param hpReader the writer being used to write the table's schema to its
     *        header page
     *
     * @param typeID the unsigned-byte value read from the table's header page,
     *        corresponding to this key's type.  Although this value is already
     *        parsed before calling this method, it also contains flags that
     *        this method handles, so it must be passed in as well.
     *
     * @param type the constraint type, either
     *        {@link TableConstraintType#PRIMARY_KEY} or
     *        {@link TableConstraintType#FOREIGN_KEY}.
     *
     * @return a specification of the key, including its name, what columns
     *         appear in the key, and what index is used to enforce the key
     *
     * @throws IllegalArgumentException if the <tt>type</tt> argument is
     *         <tt>null</tt>, or is not one of the accepted values
     */
    private KeyColumnIndexes readKey(PageReader hpReader, int typeID,
                                     TableConstraintType type) {

        if (type == TableConstraintType.PRIMARY_KEY) {
            logger.debug(" * Reading primary key");
        }
        else if (type == TableConstraintType.UNIQUE) {
            logger.debug(" * Reading candidate key");
        }
        else {
            throw new IllegalArgumentException(
                "Invalid TableConstraintType value " + type);
        }

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = hpReader.readVarString255();

        int keySize = hpReader.readUnsignedByte();
        int[] keyCols = new int[keySize];
        for (int i = 0; i < keySize; i++)
            keyCols[i] = hpReader.readUnsignedByte();

        // This should always be specified.
        String indexName = hpReader.readVarString255();
        
        KeyColumnIndexes key = new KeyColumnIndexes(indexName, keyCols);
        key.setConstraintName(constraintName);
        
        return key;
    }    


    private ForeignKeyColumnIndexes readForeignKey(PageReader hpReader, int typeID) {
        logger.debug(" * Reading foreign key");

        String constraintName = null;
        if ((typeID & 0x80) != 0)
            constraintName = hpReader.readVarString255();

        String refTableName = hpReader.readVarString255();
        int keySize = hpReader.readUnsignedByte();

        int[] keyCols = new int[keySize];
        int[] refCols = new int[keySize];
        for (int i = 0; i < keySize; i++) {
            keyCols[i] = hpReader.readUnsignedByte();
            refCols[i] = hpReader.readUnsignedByte();
        }

        ForeignKeyColumnIndexes fk = new ForeignKeyColumnIndexes(
            keyCols, refTableName, refCols);
        fk.setConstraintName(constraintName);

        return fk;
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
	
	public void printTable(TableFileInfo tblFileInfo) throws IOException {
		BlockColumnStoreReader reader = new BlockColumnStoreReader();
		
		for (int i = 0; i < tblFileInfo.getSchema().numColumns(); i++) {
			logger.debug("Column " + i);
			DBPage currentPage = reader.getFirstDataPage(tblFileInfo, i);
			while (currentPage != null) {
				ColStoreBlock currentBlock = reader.getFirstBlockInPage(
					tblFileInfo, currentPage, i);
				while (currentBlock != null) {
					Object current = currentBlock.getNext();
					while (current != null) {
						logger.debug(current);
						current = currentBlock.getNext();
					}
					currentBlock = reader.getNextBlockInPage(tblFileInfo, 
						currentPage, i, currentBlock);
				}
				currentPage = reader.getNextDataPage(tblFileInfo, currentPage, i);
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
		
		int distincts = analyzer.getCounts(index) + 1;
		int bitsize = (int) Math.ceil(Math.log(distincts)/Math.log(2));
		
		logger.debug("Bitsize " + bitsize);
		
		int blockNum = (int) Math.floor(16.0 / bitsize);
		
		int val = 1;
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
		
		logger.debug(dict);
		
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
