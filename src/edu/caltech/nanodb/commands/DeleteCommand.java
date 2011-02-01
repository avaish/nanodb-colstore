package edu.caltech.nanodb.commands;


import java.io.IOException;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.qeval.Planner;
import edu.caltech.nanodb.qeval.TupleProcessor;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.TableManager;


/**
 * This command object represents a top-level <tt>DELETE</tt> command issued
 * against the database.  <tt>DELETE</tt> commands are pretty simple, having a
 * single form:   <tt>DELETE FROM ... [WHERE ...]</tt>.
 * <p>
 * Execution of this command is relatively straightforward; the real nuance is
 * that we can treat it as a "select for deletion", and possibly perform the
 * deletion in an optimized manner (particularly if the <tt>WHERE</tt> clause
 * contains subqueries).  Internally, we treat it as a <tt>SELECT</tt>, and
 * each row returned is deleted from the specified table.
 */
public class DeleteCommand extends QueryCommand {

    /**
     * An implementation of the tuple processor interface used by the
     * {@link DeleteCommand} to delete each tuple.
     */
    private static class TupleRemover implements TupleProcessor {
        /** The table manager to use to delete tuples. */
        private TableManager tableMgr;

        /** The table whose tuples will be deleted. */
        private TableFileInfo tblFileInfo;

        /**
         * Initialize the tuple-remover object with the details it needs to
         * delete tuples from the specified table.
         *
         * @param tblFileInfo details of the table that will be modified
         */
        public TupleRemover(TableFileInfo tblFileInfo) {
            this.tblFileInfo = tblFileInfo;
            this.tableMgr = tblFileInfo.getTableManager();
        }

        /** This tuple-processor implementation doesn't care about the schema. */
        public void setSchema(Schema schema) {
            // Ignore.
        }

        /** This implementation simply deletes each tuple it is handed. */
        public void process(Tuple tuple) throws IOException {
            tableMgr.deleteTuple(tblFileInfo, tuple);
        }
    }


    /** The name of the table that the data will be deleted from. */
    private String tableName;


    /**
     * If a <tt>WHERE</tt> expression is specified, this field will refer to
     * the expression to be evaluated.
     */
    private Expression whereExpr = null;


    TableFileInfo tblFileInfo;


    /**
     * Constructs a new delete command.
     *
     * @param tableName the name of the table from which we will be deleting
     *        tuples
     *
     * @param whereExpr the predicate governing which rows will be deleted
     */
    public DeleteCommand(String tableName, Expression whereExpr) {
        super(QueryCommand.Type.DELETE);

        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        this.tableName = tableName;
        this.whereExpr = whereExpr;
    }


    protected void prepareQueryPlan() throws IOException, SchemaNameException {

        tblFileInfo = StorageManager.getInstance().openTable(tableName);

        // Create a plan for executing the SQL query.
        Planner planner = new Planner();
        plan = planner.makeLeafSelect(tableName, whereExpr);
    }


    protected TupleProcessor getTupleProcessor() {
        return new TupleRemover(tblFileInfo);
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DeleteCommand[table=");

        sb.append(tableName);

        if (whereExpr != null) {
            sb.append(", whereExpr=\"");
            sb.append(whereExpr);
            sb.append("\"");
        }
        sb.append(']');

        return sb.toString();
    }
}
