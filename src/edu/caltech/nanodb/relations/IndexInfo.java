package edu.caltech.nanodb.relations;


import java.util.ArrayList;

/**
 * This class holds all details of an index within the database.
 */
public class IndexInfo {
    /** The unique name of this index. */
    private String indexName;
    

    /** The name of the table that the index is built against. */
    private String tableName;
    

    /** The schema for the table that the index is built against. */
    private Schema tableSchema;


    /**
     * A flag indicating whether the index is a unique index (i.e. each value
     * appears only once) or not.
     */
    private boolean unique;


    /** The table columns that the index is built against. */
    private ArrayList<String> indexColumns;
}
