package edu.caltech.nanodb.storage.colstore;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageReader;
import edu.caltech.nanodb.storage.heapfile.DataPage;

public class CSDataPage {
	private static Logger logger = Logger.getLogger(DataPage.class);

    public static final int ENCODING_OFFSET = 2;
    
    public static final int COUNT_OFFSET = 6;
	
	public static int getEncoding(DBPage dbPage) {
		PageReader reader = new PageReader(dbPage);
		reader.setPosition(ENCODING_OFFSET);
		return reader.readInt();
	}
	
	public static int getCount(DBPage dbPage) {
		PageReader reader = new PageReader(dbPage);
		reader.setPosition(COUNT_OFFSET);
		return reader.readInt();
	}
}
