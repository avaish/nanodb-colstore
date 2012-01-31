package edu.caltech.nanodb.qeval;


import java.io.IOException;

import java.util.List;

import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;

import edu.caltech.nanodb.expressions.Expression;

import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.plans.SortNode;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;

import org.apache.log4j.Logger;


/**
 * This class generates execution plans for performing SQL queries.  The
 * primary responsibility is to generate plans for SQL <tt>SELECT</tt>
 * statements, but <tt>UPDATE</tt> and <tt>DELETE</tt> expressions can also
 */
public class SimplePlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


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

        PlanNode plan;

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

        // If we have a WHERE clause, we have two choices.  If the current plan
        // is a simple file-scan then put the predicate on the file-scan.
        // Otherwise, put a select node above the current plan.
        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null) {
            if (plan instanceof FileScanNode) {
                FileScanNode fileScan = (FileScanNode) plan;

                if (fileScan.predicate != null) {
                    // There is already an existing predicate.  Add this as a
                    // conjunct to the existing predicate.
                    Expression pred = fileScan.predicate;
                    boolean handled = false;

                    // If the current predicate is an AND operation, just make
                    // the where-expression an additional term.
                    if (pred instanceof BooleanOperator) {
                        BooleanOperator bool = (BooleanOperator) pred;
                        if (bool.getType() == BooleanOperator.Type.AND_EXPR) {
                            bool.addTerm(whereExpr);
                            handled = true;
                        }
                    }

                    if (!handled) {
                        // Oops, the current file-scan predicate wasn't an AND.
                        // Create an AND expression instead.
                        BooleanOperator bool =
                            new BooleanOperator(BooleanOperator.Type.AND_EXPR);
                        bool.addTerm(pred);
                        bool.addTerm(whereExpr);
                        fileScan.predicate = bool;
                    }
                }
                else {
                    // Simple - just add where-expression onto the file-scan.
                    fileScan.predicate = whereExpr;
                }
            }
            else {
                // The subplan is more complex, so put a filter node above it.
                plan = new SimpleFilterNode(plan, whereExpr);
            }
        }

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


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     * Depending on the clause's {@link FromClause#getClauseType type},
     * the plan tree will comprise varying operations, such as:
     * <ul>
     *   <li>
     *     {@link edu.caltech.nanodb.commands.FromClause.ClauseType#BASE_TABLE} -
     *     the clause is a simple table reference, so a simple select operation
     *     is constructed via {@link #makeSimpleSelect}.
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
                plan = makeSimpleSelect(fromClause.getTableName(), null);
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

            // If it's a NATURAL join, or a join with a USING clause, project
            // out the duplicate column names.
            List<SelectValue> selectValues =
                fromClause.getPreparedSelectValues();

            if (selectValues != null)
                plan = new ProjectNode(plan, selectValues);

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
    public SelectNode makeSimpleSelect(String tableName,
                                       Expression predicate) throws IOException {
        
        // Open the table.
        TableFileInfo tableInfo = StorageManager.getInstance().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode node = new FileScanNode(tableInfo, predicate);

        return node;
    }
}
