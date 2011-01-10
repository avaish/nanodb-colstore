package edu.caltech.nanodb.commands;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.relations.ColumnInfo;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.Cost;

import edu.caltech.nanodb.qeval.EvalStats;
import edu.caltech.nanodb.qeval.Planner;
import edu.caltech.nanodb.qeval.QueryEvaluator;
import edu.caltech.nanodb.qeval.TupleProcessor;

import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This command object represents a top-level <code>SELECT</code> command
 * issued against the database.  This class is <em>not</em> used to represent
 * a subquery; subqueries are represented with the {@link SelectClause}.  And
 * this is what the <code>SelectCommand</code> uses internally to represent
 * the query itself.
 *
 * @see SelectClause
 */
public class SelectCommand extends QueryCommand {

    private static class TuplePrinter implements TupleProcessor {

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

        // Create a plan for executing the SQL query.
        Planner planner = new Planner();
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

