package edu.caltech.nanodb.qeval;


import java.util.List;

import edu.caltech.nanodb.plans.PlanNode;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Tuple;


public class QueryEvaluator {

    /**
     * @return An object containing statistics about the plan evaluation.
     */
    public static EvalStats executePlan(PlanNode plan, TupleProcessor processor)
        throws Exception {

        // Execute the query and print out the results.

        long startTime = System.nanoTime();

        plan.initialize();

        // TODO:  Fold this into the initialization code...
        List<ColumnInfo> resultSchema = plan.getColumnInfos();

        Tuple tuple = null;
        int rowsProduced = 0;

        try {
            while (true) {
                // Get the next tuple.  If there aren't anymore, we're done!
                tuple = plan.getNextTuple();
                if (tuple == null)
                    break;

                rowsProduced++;

                //plan.environment.setCString tableName, Tuple tuple

                // Do whatever we're supposed to do with the tuple.
                processor.process(tuple);
            }
        }
        finally {
            plan.cleanUp();
        }
        long elapsedTimeNanos = System.nanoTime() - startTime;

        return new EvalStats(rowsProduced, elapsedTimeNanos);
    }
}

