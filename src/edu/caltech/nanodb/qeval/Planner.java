package edu.caltech.nanodb.qeval;


import java.io.IOException;

import java.util.List;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SortNode;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanArray;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;


/**
 * This class generates execution plans for performing SQL queries.  The
 * primary responsibility is to generate plans for SQL <tt>SELECT</tt>
 * statements, but <tt>UPDATE</tt> and <tt>DELETE</tt> expressions can also
 *
 * @todo Make this into an interface.
 */
public class Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(Planner.class);


    /** BeanShell interpreter. */
    // private static Interpreter interpreter;
    //Interpreter i = new Interpreter();

    /*
    static {
        interpreter = new Interpreter();
        
        // Load the script once.
        try {
            interpreter.source("res/planscript.bsh");
            interpreter.set("logger", logger);
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (EvalError e) {
            e.printStackTrace();
        }
    }
    */


    /** Constructs a Planner object to use for generating plans. */
    public Planner() {
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause) throws IOException {

        // We want to take a simple SELECT a, b, ... FROM A, B, ... WHERE ...
        // and turn it into a tree of plan nodes.

        // The most naive approach is to do all the joins on TRUE theta conditions
        // and then select out the desired rows and finally project to the desired
        // schema.  This plan is legal for all such queries, but it is definitely
        // slow.

        PlanNode plan = null;

        // Create a subplan that generates the relation specified by the FROM
        // clause.  If there are joins in the FROM clause then this will be a
        // tree of plan-nodes.
        FromClause fromClause = selClause.getFromClause();
        if (fromClause != null) {
            plan = makeJoinTree(fromClause);
        }
        else {
            throw new UnsupportedOperationException(
                "NanoDB doesn't yet support SQL queries without a FROM clause!");
        }

        // If we have a WHERE clause, put a select node above the join node.
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null)
            plan = new SimpleFilterNode(plan, whereExpr);

        // TODO:  Grouping/aggregation will go somewhere in here.

        // Depending on the SELECT clause, create a project node at the top of
        // the tree.
        if (!selClause.isTrivialProject()) {
            List<SelectValue> selectValues = selClause.getSelectValues();
            plan = new ProjectNode(plan, selectValues);
        }

        // Finally, apply any sorting at the end.
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (!orderByExprs.isEmpty())
            plan = new SortNode(plan, orderByExprs);

        plan.prepare();

        return plan;
    }
  

    /*
     * Creates a list of SelectNodes that serve as the leaves of the tree. These
     * nodes just do a simple file scan, one per table in the query.
     *
     * @param relation the tables that need to be opened
     * @return list of select nodes that open table files
     * @throws ExecutionException if one of the tables if derived (because then
     *         it is not a leaf) or if the table manager fails to open a table
     *         (check nested exception).
    private PlanNode makeLeafSelects(FromClause relation)
        throws ExecutionException {

        PlanNode result = null;

        if (relation.isBaseTable()) {
            // Get the table name from the clause
            String table = relation.getTableName();

            try {
                // Open the table.
                TableFileInfo tableInfo = tableManager.openTable(table);

                // Make a SelectNode whose sole purpose is to read rows from the table.
                // Use a null predicate to indicate full selection
                SelectNode newNode = new SelectNode(tableInfo,
                    SelectNode.ImplementationType.FILE_SCAN, null);

                // Set the result name for this table to the from clause's result name
                newNode.resultName = relation.getResultName();

                // Set this node's environment
                newNode.environment = env;

                result = newNode;
            }
            catch (IOException e) {
                // The TableManager failed to open the db file.
                // Do some horrible error.

                throw new ExecutionException(e);
            }
        }
        else {
            result =
        }

        return result;
    }
     */


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * Depending on the clause's {@link FromClause#getClauseType type},
     * the plan tree will comprise varying operations, such as:
     * <ul>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#BASE_TABLE} -
     *     the clause is a simple table reference, so a simple select operation
     *     is constructed via {@link #makeLeafSelect}.
     *   </li>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#SELECT_SUBQUERY} -
     *     the clause is a <tt>SELECT</tt> subquery, so a plan subtree is
     *     constructed by a recursive call to {@link #makePlan}.
     *   </li>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#JOIN_EXPR} -
     *     the clause is a join of two relations, so a join operation is created
     *     between the left and right children of the from-clause.  Plans for
     *     generating the child results are constructed by recursive calls to
     *     <tt>makeJoinTree()</tt>.
     *   </li>
     * </ul>
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makeJoinTree(FromClause fromClause)
        throws IOException {

        PlanNode plan;

        FromClause.ClauseType clauseType = fromClause.getClauseType();
        switch (clauseType) {
        case BASE_TABLE:
        case SELECT_SUBQUERY:

            if (clauseType == FromClause.ClauseType.SELECT_SUBQUERY) {
                // This clause is a SQL subquery, so generate a plan from the
                // subquery and return it.
                plan = makePlan(fromClause.getSelectClause());
            }
            else {
                // This clause is a base-table, so we just generate a file-scan
                // plan node for the table.
                plan = makeLeafSelect(fromClause.getTableName(), null);
            }

            // If the FROM-clause renames the result, apply the renaming here.
            if (fromClause.isRenamed())
                plan = new RenameNode(plan, fromClause.getResultName());

            break;

        case JOIN_EXPR:
            PlanNode leftChild = makeJoinTree(fromClause.getLeftChild());
            PlanNode rightChild = makeJoinTree(fromClause.getRightChild());

            Expression joinPredicate = fromClause.getPreparedJoinExpr();

            plan = new NestedLoopsJoinNode(leftChild, rightChild,
                fromClause.getJoinType(), joinPredicate);

            break;

        default:
            throw new IllegalArgumentException(
                "Unrecognized from-clause type:  " + fromClause.getClauseType());
        }

        return plan;
    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or <tt>null</tt> if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeLeafSelect(String tableName,
        Expression predicate) throws IOException {
        
        // Open the table.
        TableFileInfo tableInfo = StorageManager.getInstance().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode node = new FileScanNode(tableInfo, predicate);

        return node;
    }



    /**
     * Generates all interesting plans to be considered.
     *
     * @param original the naive plan tree to be transformed.
     * @return a list of plan trees to be costed and executed
     */
    private PlanArray generateAlternatePlans(PlanNode original) {
        // TODO

        PlanArray plans = new PlanArray();
        plans.addPlan(original);

/*
        try {
            interpreter.set("originator", originator);
            interpreter.eval("pushConditions(originator)");;
            interpreter.eval("originator = eliminateNodes(originator)");
            originator = (PlanNode)interpreter.get("originator");
          
            interpreter.eval("plans = equivalentPlans(originator)");
            plans = (PlanArray)interpreter.get("plans");
        }
        catch (EvalError e) {
            e.printStackTrace();
        }
      
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);  // autoFlush = true
        for (int i = 0; i < plans.size(); i++) {
            plans.getPlan(i).printNodeTree(ps);
            logger.debug("PLANNER:\n" + baos.toString());
            baos.reset();
        }
      
        logger.debug("Planner generated " + plans.size() + " plans.");
        logger.debug("Planner rejected " + plans.redundantPlans() +
            " redundant plans.");
*/
        
        return plans;
    }
}
