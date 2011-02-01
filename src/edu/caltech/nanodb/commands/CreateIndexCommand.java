package edu.caltech.nanodb.commands;

import java.util.ArrayList;

/**
 * This command-class represents the <tt>CREATE INDEX</tt> DDL command.
 */
public class CreateIndexCommand extends Command {

    private String indexName;

    private String indexType;

    /**
     * This flag specifies whether the index is a unique index or not.  If the
     * value is true then no key-value may appear multiple times; if the value
     * is false then a key-value may appear multiple times.
     */
    private boolean unique;


    /** The name of the table that the index is built against. */
    private String tableName;


    /**
     * The list of column-names that the index is built against.  The order of
     * these values is important; for ordered indexes, the index records must be
     * kept in the order specified by the sequence of column names.
     */
    private ArrayList<String> columnNames = new ArrayList<String>();



    public CreateIndexCommand(String indexName, String indexType) {
        super(Type.DDL);

        if (indexName == null)
            throw new IllegalArgumentException("indexName cannot be null");

        if (indexType == null)
            throw new IllegalArgumentException("indexType cannot be null");

        this.indexName = indexName;
        this.indexType = indexType;
    }


    public void setTable(String tableName) {
        this.tableName = tableName;
    }


    public void setUnique(boolean unique) {
        this.unique = unique;
    }


    public void addColumn(String columnName) {
        this.columnNames.add(columnName);
    }


    public void execute() {
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
