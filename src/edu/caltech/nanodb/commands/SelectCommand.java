package edu.caltech.nanodb.commands;


import java.io.IOException;

import edu.caltech.nanodb.relations.ColumnInfo;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.DPJoinPlanner;
import edu.caltech.nanodb.qeval.Planner;
import edu.caltech.nanodb.qeval.TupleProcessor;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This command object represents a top-level <tt>SELECT</tt> command issued
 * against the database.  The query itself is represented by the
 * {@link SelectClause} class, particularly because a <tt>SELECT</tt> statement
 * can itself contain other <tt>SELECT</tt> statements.
 *
 * @see SelectClause
 */
public class SelectCommand extends QueryCommand {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SelectCommand.class);


    /**
     * This implementation of the tuple-processor interface simply prints out
     * the schema and tuples produced by the <tt>SELECT</tt> statement.
     */
    private static class TuplePrinter implements TupleProcessor {

        public void setSchema(Schema schema) {
            // TODO:  Print the schema.  Not like this.
            System.out.print("schema:  ");
            for (ColumnInfo colInfo : schema) {
                System.out.print(" | ");

                String colName = colInfo.getName();
                String tblName = colInfo.getTableName();

                // TODO:  To only print out table-names when the column-name is
                //        ambiguous by itself, uncomment the first part and
                //        then comment out the next part.

                // Only print out the table name if there are multiple columns
                // with this column name.
                // if (schema.numColumnsWithName(colName) > 1 && tblName != null)
                //     System.out.print(tblName + '.');

                // If table name is specified, always print it out.
                if (tblName != null)
                    System.out.print(tblName + '.');

                System.out.print(colName);
            }
            System.out.println(" |");
        }

        public void process(Tuple tuple) {
            // TODO:  Print the tuple data.  Not like this.
            System.out.print("tuple:  ");
            for (int i = 0; i < tuple.getColumnCount(); i++) {
                System.out.print(" | ");
                System.out.print(tuple.getColumnValue(i));
            }
            System.out.println(" |");

        }
    }


    /**
     * This object contains all the details of the top-level select clause,
     * including any subqueries, that is going to be evaluated.
     */
    private SelectClause selClause;


    public SelectCommand(SelectClause selClause) {
        super(QueryCommand.Type.SELECT);

        if (selClause == null)
            throw new NullPointerException("selClause cannot be null");

        this.selClause = selClause;
    }


    /**
     * Returns the root select-clause for this select command.
     *
     * @return the root select-clause for this select command
     */
    public SelectClause getSelectClause() {
        return selClause;
    }


    /**
     * Prepares the <tt>SELECT</tt> statement for evaluation by analyzing the
     * schema details of the statement, and then preparing an execution plan
     * for the statement.
     */
    protected void prepareQueryPlan() throws IOException, SchemaNameException {
        Schema resultSchema = selClause.computeSchema();
        logger.debug("Prepared SelectClause:\n" + selClause);
        logger.debug("Result schema:  " + resultSchema);

        // Create a plan for executing the SQL query.
        DPJoinPlanner planner = new DPJoinPlanner();
        plan = planner.makePlan(selClause);
    }


    protected TupleProcessor getTupleProcessor() {
        return new TuplePrinter();
    }


    @Override
    public String toString() {
        return "SelectCommand[" + selClause + "]";
    }
}

