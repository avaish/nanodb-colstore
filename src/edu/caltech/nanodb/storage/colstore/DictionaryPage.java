package edu.caltech.nanodb.storage.colstore;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FileEncoding;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.PageWriter;
import edu.caltech.nanodb.storage.heapfile.DataPage;

public class DictionaryPage {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DictionaryPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int ENCODING_MARKER = FileEncoding.DICTIONARY.ordinal();
    
    public static final int COUNT_OFFSET = 6;
    
    public static final int NEXT_BLOCK_START_OFFSET = 10;

    public static final int FIRST_BLOCK_OFFSET = 14;


    /**
     * Initialize a newly allocated dictionary page.  Currently this involves setting
     * the number of values to 0 and marking the page as an dictionary encoded page.
     *
     * @param dbPage the data page to initialize
     */
	public static void initNewPage(DBPage dbPage) {
		PageWriter dictWriter = new PageWriter(dbPage);
        dictWriter.setPosition(ENCODING_OFFSET);
        dictWriter.writeInt(ENCODING_MARKER);
        dictWriter.writeInt(0);
        dictWriter.writeInt(14);
	}

	/** Writes a dictionary block. */
	public static boolean writeBlock(DBPage dbPage, int currentBlock, int blockNum) {
		
		PageReader dictReader = new PageReader(dbPage);
    	PageWriter dictWriter = new PageWriter(dbPage);
    	
    	dictReader.setPosition(ENCODING_OFFSET);
    	
    	if (dictReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	dictReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	int write_offset = dictReader.readInt();
    	
    	if (write_offset + 2 > dbPage.getPageSize()) {
    		return false;
    	}
    	
    	dictWriter.setPosition(write_offset);
    	
    	dictWriter.writeShort(currentBlock);
    	
    	dictReader.setPosition(COUNT_OFFSET);
    	int next_write_pos = dictWriter.getPosition();
    	int count = dictReader.readInt() + blockNum;
    	
    	dictWriter.setPosition(COUNT_OFFSET);
    	dictWriter.writeInt(count);
    	dictWriter.writeInt(next_write_pos);
    	
    	return true;
	}

	/** Writes a dictionary. */
	public static void writeDictionary(DBPage dbPage, HashMap<String, Integer> dict,
			int bitsize, int blockNum, ColumnInfo info) {
		
		PageReader dictReader = new PageReader(dbPage);
    	PageWriter dictWriter = new PageWriter(dbPage);
    	
    	dictReader.setPosition(ENCODING_OFFSET);
    	
    	if (dictReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	dictReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	int write_offset = dictReader.readInt();
    	
    	dictWriter.setPosition(write_offset);
    	
    	dictWriter.writeInt(bitsize);
    	dictWriter.writeInt(blockNum);
    	dictWriter.writeInt(dict.size());
    	
    	for (String key : dict.keySet()) {
    		dictWriter.setPosition(dictWriter.getPosition() + dbPage.writeObject
    			(dictWriter.getPosition(), info.getType(), key));
    		dictWriter.writeInt(dict.get(key));
    	}
	}
	
	/** Reads dictionary bit size. */
	public static int getBitSize(DBPage dbPage) {
		PageReader dictReader = new PageReader(dbPage);
    	
    	dictReader.setPosition(ENCODING_OFFSET);
    	
    	if (dictReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	dictReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	dictReader.setPosition(dictReader.readInt());
    	
    	return dictReader.readInt();
	}
	
	/** Reads dictionary block size. */
	public static int getBlockNum(DBPage dbPage) {
		PageReader dictReader = new PageReader(dbPage);
    	
    	dictReader.setPosition(ENCODING_OFFSET);
    	
    	if (dictReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	dictReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	dictReader.setPosition(dictReader.readInt() + 4);
    	
    	return dictReader.readInt();
	}
	
	/** Reconstruct dictionary from disk. */
	public static HashMap<Integer, Object> constructDictionary(DBPage dbPage, 
		ColumnInfo info) {
	
		PageReader dictReader = new PageReader(dbPage);
    	
    	dictReader.setPosition(ENCODING_OFFSET);
    	
    	if (dictReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	dictReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	dictReader.setPosition(dictReader.readInt() + 8);
    	
    	int size = dictReader.readInt();
    	
    	HashMap<Integer, Object> dict = new HashMap<Integer, Object>();
    	Object obj;
    	int bits;
    	
    	for (int i = 0; i < size; i++) {
    		obj = dbPage.readObject(dictReader.getPosition(), info.getType());
    		dictReader.setPosition(dictReader.getPosition() + 
    			DBPage.getObjectDiskSize(obj, info.getType()));
    		bits = dictReader.readInt();
    		dict.put(bits, obj);
    	}
		
		return dict;
	}
	
	/** Read block from disk. */
	public static int getBlockEncodedData(DBPage dbPage, int blockStart) {
		PageReader dictReader = new PageReader(dbPage);
    	
    	dictReader.setPosition(ENCODING_OFFSET);
    	
    	if (dictReader.readInt() != ENCODING_MARKER) {
    		throw new IllegalArgumentException("Wrong encoding type");
    	}
    	
    	dictReader.setPosition(NEXT_BLOCK_START_OFFSET);
    	
    	if (dictReader.readInt() <= blockStart) {
    		return Integer.MIN_VALUE;
    	}
    	
    	dictReader.setPosition(blockStart);
    	
    	return dictReader.readShort();
	}
	
	/** Read first block from disk. */
	public static int getFirstBlockEncodedData(DBPage dbPage) {
		return getBlockEncodedData(dbPage, FIRST_BLOCK_OFFSET);
	}
}
