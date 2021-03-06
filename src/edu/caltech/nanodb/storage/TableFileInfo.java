package edu.caltech.nanodb.storage;


import java.util.ArrayList;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * This class is used to hold information about a single table in the database.
 * It stores the table's name, the schema details of the table, and the
 * {@link DBFile} object where the table's data is actually stored.
 */
public class TableFileInfo {
    /** If a table name isn't specified, this value is used instead. */
    public static final String UNNAMED_TABLE = "(unnamed)";


    /** The name of this table. */
    private String tableName;


    /** The schema of this table file. */
    private TableSchema schema;


    /** The statistics stored in this table file. */
    private TableStats stats;


    /** The table manager used to access this table file. */
    private TableManager tableManager;
    
    
    /** The file type of the DBFile(s). */
    private DBFileType fileType;


    /**
     * If the table file has been opened, this is the actual data file that
     * the table is stored in.  Otherwise, this will be <tt>null</tt>.
     */
    private ArrayList<DBFile> dbFiles = new ArrayList<DBFile>();
    
    
    private int dbFileCount = 0;


    /**
     * Construct a table file information object that represents the specified
     * table name and on-disk database file object.
     *
     * @param tableName the name of the table that this object represents
     *
     * @param dbFile the database file that holds the table's data
     *
     * @review (donnie) Shouldn't this just load the column info from the
     *         specified DBFile instance?
     */
    public TableFileInfo(String tableName, DBFile dbFile) {
        if (tableName == null)
            tableName = UNNAMED_TABLE;

        this.tableName = tableName;
        this.dbFiles.add(dbFile);
        this.dbFileCount = 1;

        schema = new TableSchema();
        stats = new TableStats(schema.numColumns());
    }


    /**
     * Construct a table file information object for the specified table name.
     * This constructor is used by the <tt>CREATE TABLE</tt> command to hold the
     * table's schema, before the table has actually been created.  After the
     * table is created, the {@link #setDBFile} method is used to store the
     * database-file object onto this object.
     *
     * @param tableName the name of the table that this object represents
     */
    public TableFileInfo(String tableName) {
        this(tableName, null);
    }


    /**
     * Returns the actual database file that holds this table's data. Retained
     * for compatibility.
     *
     * @return the actual database file that holds this table's data, or
     *         <tt>null</tt> if it hasn't yet been set.
     */
    public DBFile getDBFile() {
        return dbFiles.get(0);
    }


    /**
     * Method for storing the database-file object onto this table-file
     * information object, for example after successful completion of a
     * <tt>CREATE TABLE</tt> command. Retained for compatibility.
     *
     * @param dbFile the database file that the table's data is stored in.
     */
    public void setDBFile(DBFile dbFile) {

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile must not be null!");

        if (this.dbFiles.get(0) != null)
            throw new IllegalStateException("This object already has a dbFile!");

        this.dbFiles.set(0, dbFile);
        this.dbFileCount = 1;
    }
    
    
    /**
     * Method for storing database-file objects onto this table-file
     * information object, for example after successful completion of a
     * <tt>CREATE TABLE</tt> command.
     *
     * @param dbFile the database file that the table's data is stored in.
     */
    public void addDBFile(DBFile dbFile) {
    	if (dbFile == null)
            throw new IllegalArgumentException("dbFile must not be null!");
    	
    	this.dbFiles.add(dbFile);
    	this.dbFileCount++;
    }


    /**
     * Returns the associated table name.
     *
     * @return the associated table name
     */
    public String getTableName() {
        return tableName;
    }


    public TableManager getTableManager() {
        return tableManager;
    }


    public void setTableManager(TableManager tableManager) {
        this.tableManager = tableManager;
    }


    /**
     * Returns the schema object associated with this table.  Note that this is
     * not a copy of the schema; it can be modified if so desired.  This is
     * necessary for table creation and modification.
     *
     * @return the schema object describing this table's schema
     */
    public TableSchema getSchema() {
        return schema;
    }


    public TableStats getStats() {
        return stats;
    }


    public void setStats(TableStats stats) {
        if (stats == null)
            throw new NullPointerException("stats cannot be null");

        this.stats = stats;
    }


	public DBFileType getFileType() {
		return fileType;
	}


	public void setFileType(DBFileType fileType) {
		this.fileType = fileType;
	}


	/**
     * Returns a file that holds this table's data. 
     * 
     * @param i the index of the data file.
     *
     * @return the actual database file that holds this table's data, or
     *         <tt>null</tt> if it hasn't yet been set.
     */
	public DBFile getDBFile(int i) {
		return dbFiles.get(i);
	}

	/**
     * Returns all of the files that hold this table's data. 
     *
     * @return all of the actual database files that hold this table's data
     */
	public ArrayList<DBFile> dbFiles() {
		// TODO Auto-generated method stub
		return dbFiles;
	}
}
