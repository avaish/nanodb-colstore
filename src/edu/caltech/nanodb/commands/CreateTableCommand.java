package edu.caltech.nanodb.commands;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnInfo;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * This command handles the <tt>CREATE TABLE</tt> DDL operation.
 */
public class CreateTableCommand extends Command {

    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(CreateTableCommand.class);


    /** Name of the table to be created. */
    private String tableName;

    /** If this flag is <tt>true</tt> then the table is a temporary table. */
    private boolean temporary;


    /**
     * If this flag is <tt>true</tt> then the create-table operation should only
     * be performed if the specified table doesn't already exist.
     */
    private boolean ifNotExists;


    /** List of column-declarations for the new table. */
    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();

    /** List of constraints for the new table. */
    private List<ConstraintDecl> constraints = new ArrayList<ConstraintDecl>();


    /**
     * Create a new object representing a <tt>CREATE TABLE</tt> statement.
     *
     * @param tableName the name of the table to be created
     */
    public CreateTableCommand(String tableName,
                              boolean temporary, boolean ifNotExists) {
        super(Command.Type.DDL);

        this.tableName = tableName;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
    }


    /**
     * Adds a column description to this create-table command.  This method is
     * primarily used by the SQL parser.
     *
     * @param colInfo the details of the column to add
     *
     * @throws NullPointerException if colDecl is null
     */
    public void addColumn(ColumnInfo colInfo) {
        if (colInfo == null)
            throw new NullPointerException("colInfo");

        columnInfos.add(colInfo);
    }


    /**
     * Adds a constraint to this create-table command.  This method is primarily
     * used by the SQL parser.
     *
     * @param con the details of the table constraint to add
     *
     * @throws NullPointerException if con is null
     */
    public void addConstraint(ConstraintDecl con) {
        if (con == null)
            throw new NullPointerException("con");

        constraints.add(con);
    }


    public void execute() throws ExecutionException {
        StorageManager storageManager = StorageManager.getInstance();

        // See if the table already exists.
        if (ifNotExists) {
            logger.debug("Checking if table " + tableName + " already exists.");

            try {
                storageManager.openTable(tableName);

                // If we got here then the table exists.  Skip the operation.
                System.out.println("Table " + tableName + " already exists.");
                return;
            }
            catch (FileNotFoundException e) {
                // Table doesn't exist yet!  This is an expected exception.
            }
            catch (IOException e) {
                // Some other unexpected exception occurred.  Report an error.
                throw new ExecutionException(
                    "Exception while trying to determine if table " +
                    tableName + " exists.", e);
            }
        }

        // Set up the table-file info based on the command details.

        logger.debug("Creating a TableFileInfo object describing the new table " +
            tableName + ".");
        TableFileInfo tblFileInfo = new TableFileInfo(tableName);
        for (ColumnInfo colInfo : columnInfos) {
            try {
                tblFileInfo.getSchema().addColumnInfo(colInfo);
            }
            catch (IllegalArgumentException iae) {
                throw new ExecutionException("Duplicate or invalid column \"" +
                    colInfo.getName() + "\".", iae);
            }
        }

        // TODO:  Add constraints to the table-file info.

        // Get the table manager and create the table.

        logger.debug("Creating the new table " + tableName + " on disk.");
        try {
            StorageManager.getInstance().createTable(tblFileInfo);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not create table \"" + tableName +
                "\".  See nested exception for details.", ioe);
        }
        logger.debug("New table " + tableName + " is created!");

        System.out.println("Created table:  " + tableName);
    }


    public String toString() {
        return "CreateTable[" + tableName + "]";
    }


    /**
     * Returns a verbose, multi-line string containing all of the details of
     * this table.
     *
     * @return a detailed description of the table described by this command
     */
    public String toVerboseString() {
        StringBuffer strBuf = new StringBuffer();

        strBuf.append(toString());
        strBuf.append('\n');

        for (ColumnInfo colInfo : columnInfos) {
            strBuf.append('\t');
            strBuf.append(colInfo.toString());
            strBuf.append('\n');
        }

        for (ConstraintDecl con : constraints) {
            strBuf.append('\t');
            strBuf.append(con.toString());
            strBuf.append('\n');
        }

        return strBuf.toString();
    }
}
