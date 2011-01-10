package edu.caltech.nanodb.commands;


import java.io.IOException;

import edu.caltech.nanodb.storage.StorageManager;


/** This Command class represents the <tt>DROP TABLE</tt> SQL command. */
public class DropTableCommand extends Command {

    /** The name of the table to drop from the database. */
    private String tableName;

    /**
     * Construct a drop-table command for the named table.
     *
     * @param tableName the name of the table to drop.
     */
    public DropTableCommand(String tableName) {
        super(Command.Type.DDL);
        this.tableName = tableName;
    }


    /**
     * Get the name of the table to be dropped.
     *
     * @return the name of the table to drop
     */
    public String getTableName() {
        return tableName;
    }


    /**
     * This method executes the <tt>DROP TABLE</tt> command by calling the
     * {@link StorageManager#dropTable} method with the specified table name.
     *
     * @throws ExecutionException if the table doesn't actually exist, or if the
     *         table cannot be deleted for some reason.
     */
    public void execute() throws ExecutionException {
        // Get the table manager, and delete the table.
        try {
            StorageManager.getInstance().dropTable(tableName);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not drop table \"" + tableName +
                "\".", ioe);
        }
    }


    @Override
    public String toString() {
        return "DropTable[" + tableName + "]";
    }
}
