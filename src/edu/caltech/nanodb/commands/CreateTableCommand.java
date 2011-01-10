package edu.caltech.nanodb.commands;


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

    /** List of column-declarations for the new table. */
    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();

    /** List of constraints for the new table. */
    private List<ConstraintDecl> constraints = new ArrayList<ConstraintDecl>();


    /**
     * Create a new object representing a <tt>CREATE TABLE</tt> statement.
     *
     * @param tableName the name of the table to be created
     */
    public CreateTableCommand(String tableName) {
        super(Command.Type.DDL);

        this.tableName = tableName;
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
