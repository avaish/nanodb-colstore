package edu.caltech.nanodb.relations;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;


/**
 * This class extends the <tt>Schema</tt> class with features specific to
 * tables, such as the ability to specify primary-key, foreign-key, and other
 * candidate-key constraints.
 */
public class TableSchema extends Schema {
    /**
     * This class represents a primary key or other unique key, specifying the
     * indexes of the columns in the key.  The class also specifies the index
     * used to enforce the key in the database.
     */
    public static class KeyColumns {
        /** This array holds the indexes of the columns in the key. */
        private int[] colIndexes;

        /** This is the name of the index that is used to enforce the key. */
        private String indexName;


        public KeyColumns(int[] colIndexes, String indexName) {
            this.colIndexes = colIndexes;
            this.indexName = indexName;
        }


        public KeyColumns(int[] colIndexes) {
            this(colIndexes, null);
        }
    }


    /**
     * This class represents a foreign key to another table in the database.
     */
    public static class ForeignKeyColumns {
        /** This array holds the indexes of the columns in the foreign key. */
        private int[] colIndexes;

        /** This is the name of the table that is referenced by this table. */
        private String referencedTable;

        /** These are the indexes of the columns in the referenced table. */
        private int[] referencedColIndexes;


        public ForeignKeyColumns(int[] colIndexes, String referencedTable,
                                 int[] referencedColIndexes) {
            if (colIndexes == null) {
                throw new IllegalArgumentException(
                    "colIndexes must be specified");
            }

            if (referencedTable == null) {
                throw new IllegalArgumentException(
                    "referencedTable must be specified");
            }
            
            if (referencedColIndexes == null) {
                throw new IllegalArgumentException(
                    "referencedColIndexes must be specified");
            }
            
            if (colIndexes.length != referencedColIndexes.length) {
                throw new IllegalArgumentException(
                    "colIndexes and referencedColIndexes must have the same length");
            }

            this.colIndexes = colIndexes;
            this.referencedTable = referencedTable;
            this.referencedColIndexes = referencedColIndexes;
        }
    }


    private KeyColumns primaryKey;


    private ArrayList<KeyColumns> candidateKeys;


    private ArrayList<ForeignKeyColumns> foreignKeys;


    public void setPrimaryKey(KeyColumns pk) {
        primaryKey = pk;
    }


    public void addCandidateKey(KeyColumns ck) {
        if (candidateKeys == null)
            candidateKeys = new ArrayList<KeyColumns>();

        candidateKeys.add(ck);
    }


    public void addForeignKey(ForeignKeyColumns fk) {
        if (foreignKeys == null)
            foreignKeys = new ArrayList<ForeignKeyColumns>();

        foreignKeys.add(fk);
    }


    private int[] getColumnIndexes(List<String> columnNames) {
        int[] result = new int[columnNames.size()];
        HashSet<String> s = new HashSet<String>();

        int i = 0;
        for (String colName : columnNames) {
            if (!s.add(colName)) {
                throw new SchemaNameException(String.format(
                    "Column %s was specified multiple times", colName));
            }
            result[i] = getColumnIndex(columnNames.get(i));
        }

        return result;
    }


    public KeyColumns makeKey(List<String> columnNames) {
        return new KeyColumns(getColumnIndexes(columnNames));
    }


    public ForeignKeyColumns makeForeignKey(List<String> columnNames,
        String refTableName, TableSchema refTableSchema,
        List<String> refColumnNames) {

        int[] colIndexes = getColumnIndexes(columnNames);
        int[] refColIndexes = refTableSchema.getColumnIndexes(refColumnNames);

        return new ForeignKeyColumns(colIndexes, refTableName, refColIndexes);
    }
}
