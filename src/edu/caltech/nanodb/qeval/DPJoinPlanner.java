package edu.caltech.nanodb.qeval;


import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;

import edu.caltech.nanodb.expressions.BooleanOperator;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.plans.SortNode;

import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;

import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TableFileInfo;

import org.apache.log4j.Logger;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations don't
 * currently span multiple subqueries.
 */
public class DPJoinPlanner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DPJoinPlanner.class);


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in the
     * <tt>FROM</tt>-clause of the query.  However, the planner will attempt to
     * push conjuncts down the plan as far as possible, so even if a leaf is a
     * base table, the plan may be a bit more complex than just a single
     * file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not be
         * used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to this
         *        leaf, or it may be nonempty if some conjuncts apply solely to
         *        this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<PlanNode>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause) throws IOException {

        // We want to take a simple SELECT a, b, ... FROM A, B, ... WHERE ...
        // and turn it into a tree of plan nodes.

        if (selClause.getFromClause() == null) {
            throw new UnsupportedOperationException(
                "NanoDB doesn't yet support SQL queries without a FROM clause!");
        }

        // Collect all predicates associated with this select-clause, from the
        // WHERE condition and all join conditions.

        HashSet<Expression> allConjuncts = new HashSet<Expression>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<FromClause>();
        collectDetails(selClause, allConjuncts, leafFromClauses);

        logger.debug("Collected conjuncts:  " + allConjuncts);
        logger.debug("Collected FROM-clauses:  " + leafFromClauses);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaf nodes.");
        ArrayList<JoinComponent> leafComponents =
            generateLeafJoinComponents(leafFromClauses, allConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:  " +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.
        JoinComponent optimalJoin = generateOptimalJoin(leafComponents, allConjuncts);
        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        // If there are any unused predicates then we need to add those into
        // the final result.

        HashSet<Expression> unusedConjuncts = new HashSet<Expression>(allConjuncts);
        unusedConjuncts.removeAll(optimalJoin.conjunctsUsed);

        Expression finalPredicate = makePredicate(unusedConjuncts);
        if (finalPredicate != null)
            plan = addPredicateToPlan(plan, finalPredicate);

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
     * This helper method pulls the essential details for join optimization out
     * of a <tt>SELECT</tt> clause.  A <tt>WHERE</tt> predicate will be added to
     * the set of conjuncts, and any <tt>FROM</tt> terms will be added into the
     * set of from-clauses.
     * <p>
     * Note that the {@link #collectDetails(FromClause, HashSet, ArrayList)}
     * method is used by this method to collect details out of a from-clause.
     *
     * @param selClause the select-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(SelectClause selClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        Expression whereExpr = selClause.getWhereExpr();
        if (whereExpr != null)
            addConjuncts(conjuncts, whereExpr);

        FromClause fromClause = selClause.getFromClause();
        if (fromClause != null)
            collectDetails(fromClause, conjuncts, leafFromClauses);
    }


    /**
     * This helper method pulls the essential details for join optimization out
     * of a <tt>FROM</tt> clause.  <tt>FROM</tt> terms will be added into the
     * set of from-clauses.
     * <p>
     * Note that this method is used by the
     * {@link #collectDetails(SelectClause, HashSet, ArrayList)}
     * to collect details out of a from-clause.  In addition, this method calls
     * itself recursively when a from-clause has child from-clauses.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        if (fromClause.getClauseType() == FromClause.ClauseType.JOIN_EXPR) {
            // This is a join expression.  Pull out the conjuncts if there are
            // any, and then collect details from both children of the join.

            FromClause.JoinConditionType condType = fromClause.getConditionType();
            if (condType != null) {
                Expression joinExpr;
                switch (condType) {
                case NATURAL_JOIN:
                case JOIN_USING:
                    joinExpr = fromClause.getPreparedJoinExpr();
                    break;

                case JOIN_ON_EXPR:
                    joinExpr = fromClause.getOnExpression();
                    break;

                default:
                    throw new IllegalStateException(
                        "Unrecognized join condition type " + condType);
                }
                addConjuncts(conjuncts, joinExpr);
            }

            collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
            collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
        }
        else {
            // This is either a base table or a derived table (specifically, a
            // SELECT subquery).  Add it to the list of leaf FROM-clauses.
            leafFromClauses.add(fromClause);
        }
    }


    /**
     * This helper method takes a predicate <tt>expr</tt> and stores all of its
     * conjuncts into the specified collection of conjuncts.  Specifically, if
     * the predicate is a Boolean <tt>AND</tt> operation then each term will
     * individually be added to the collection of conjuncts.  Any other kind of
     * predicate will be stored as-is into the collection.
     *
     * @param conjuncts the collection of conjuncts to add the predicate (or its
     *        components) to.
     *
     * @param expr the expression to pull the conjuncts out of
     */
    private void addConjuncts(Collection<Expression> conjuncts, Expression expr) {
        if (expr instanceof BooleanOperator) {
            BooleanOperator boolExpr = (BooleanOperator) expr;
            if (boolExpr.getType() == BooleanOperator.Type.AND_EXPR) {
                for (int iTerm = 0; iTerm < boolExpr.getNumTerms(); iTerm++)
                    conjuncts.add(boolExpr.getTerm(iTerm));
            }
            else {
                // The Boolean expression is an OR or NOT, so we can't add the
                // terms themselves.
                conjuncts.add(expr);
            }
        }
        else {
            // The predicate is not a Boolean expression, so just store it.
            conjuncts.add(expr);
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param allConjuncts the collection of all conjuncts found in the query
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> allConjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        logger.info("Generating plans for all leaf nodes.");
        ArrayList<JoinComponent> leafComponents = new ArrayList<JoinComponent>();
        for (FromClause leafClause : leafFromClauses) {
            PlanNode leafPlan = makeFromPlan(leafClause);
            leafPlan.prepare();
            Schema leafSchema = leafPlan.getSchema();

            // Construct a predicate for this leaf node, if possible, by adding
            // conjuncts that are specific to only this leaf plan-node.
            //
            // Do not remove those conjuncts from the set of unused conjuncts.

            HashSet<Expression> leafConjuncts = new HashSet<Expression>();
            findExprsUsingSchemas(allConjuncts, false, leafConjuncts, leafSchema);

            Expression leafPredicate = makePredicate(leafConjuncts);
            if (leafPredicate != null) {
                leafPlan = addPredicateToPlan(leafPlan, leafPredicate);
                leafPlan.prepare();
            }

            logger.info("Generated leaf plan:\n" +
                PlanNode.printNodeTreeToString(leafPlan));

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param allConjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, HashSet<Expression> allConjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans =
            new HashMap<HashSet<PlanNode>, JoinComponent>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            // This is the set of "next plans" we will generate!  Plans only get
            // stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<HashSet<PlanNode>, JoinComponent>();

            // Iterate over each plan in the current set.  Those plans already
            // join n leaf-plans together.  We will generate more plans that
            // join n+1 leaves together.
            for (JoinComponent prevComponent : joinPlans.values()) {
                HashSet<PlanNode> prevLeavesUsed = prevComponent.leavesUsed;
                PlanNode prevPlan = prevComponent.joinPlan;
                HashSet<Expression> prevConjunctsUsed = prevComponent.conjunctsUsed;
                Schema prevSchema = prevPlan.getSchema();

                // Iterate over the leaf plans; try to add each leaf-plan to
                // this join-plan, to produce new plans that join n+1 leaves.
                for (JoinComponent leaf : leafComponents) {
                    PlanNode leafPlan = leaf.joinPlan;

                    // If the leaf-plan already appears in this join, skip it!
                    if (prevLeavesUsed.contains(leafPlan))
                        continue;

                    // The new plan we generate will involve everything from the
                    // old plan, plus the leaf-plan we are joining in.
                    // Of course, we could join in different orders, so consider
                    // both join-orderings.

                    HashSet<PlanNode> newLeavesUsed =
                        new HashSet<PlanNode>(prevLeavesUsed);
                    newLeavesUsed.add(leafPlan);

                    // Compute the join predicate between these two subplans.
                    // (We presume that the subplans have both been prepared.)

                    Schema leafSchema = leafPlan.getSchema();

                    // Find the conjuncts that reference both subplans' schemas.
                    // Also remove those predicates from the original set of all
                    // conjuncts.

                    // These are the conjuncts already used by the subplans.
                    HashSet<Expression> subplanConjuncts =
                        new HashSet<Expression>(prevConjunctsUsed);
                    subplanConjuncts.addAll(leaf.conjunctsUsed);

                    // These are the conjuncts still unused for this join pair.
                    HashSet<Expression> unusedConjuncts =
                        new HashSet<Expression>(allConjuncts);
                    unusedConjuncts.removeAll(subplanConjuncts);

                    // These are the conjuncts relevant for the join pair.
                    HashSet<Expression> joinConjuncts = new HashSet<Expression>();
                    findExprsUsingSchemas(unusedConjuncts, true, joinConjuncts,
                        leafSchema, prevSchema);

                    Expression joinPredicate = makePredicate(joinConjuncts);

                    // Join the leaf-plan with the previous optimal plan, and
                    // see if it's better than whatever we currently have.
                    // We only build LEFT-DEEP join plans; the leaf node goes
                    // on the right!

                    NestedLoopsJoinNode newJoinPlan =
                        new NestedLoopsJoinNode(prevPlan, leafPlan,
                        JoinType.INNER, joinPredicate);
                    newJoinPlan.prepare();
                    PlanCost newJoinCost = newJoinPlan.getCost();

                    joinConjuncts.addAll(subplanConjuncts);
                    JoinComponent joinComponent = new JoinComponent(newJoinPlan,
                        newLeavesUsed, joinConjuncts);

                    JoinComponent currentBest = nextJoinPlans.get(newLeavesUsed);
                    if (currentBest == null) {
                        logger.info("Setting current best-plan.");
                        nextJoinPlans.put(newLeavesUsed, joinComponent);
                    }
                    else {
                        PlanCost bestCost = currentBest.joinPlan.getCost();
                        if (newJoinCost.cpuCost < bestCost.cpuCost) {
                            logger.info("Replacing current best-plan with new plan!");
                            nextJoinPlans.put(newLeavesUsed, joinComponent);
                        }
                    }
                }
            }

            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
    }


    /**
     * This helper function takes a collection of conjuncts that should comprise
     * a predicate, and creates a predicate for evaluating these conjuncts.  The
     * exact nature of the predicate depends on the conjuncts:
     * <ul>
     *   <li>If the collection contains only one conjunct, the method simply
     *       returns that one conjunct.</li>
     *   <li>If the collection contains two or more conjuncts, the method
     *       returns a {@link BooleanOperator} that performs an <tt>AND</tt> of
     *       all conjuncts.</li>
     *   <li>If the collection contains <em>no</em> conjuncts then the method
     *       returns <tt>null</tt>.
     * </ul>
     *
     * @param conjuncts the collection of conjuncts to combine into a predicate.
     *
     * @return a predicate for evaluating the conjuncts, or <tt>null</tt> if the
     *         input collection contained no conjuncts.
     */
    private Expression makePredicate(Collection<Expression> conjuncts) {
        Expression predicate = null;
        if (conjuncts.size() == 1) {
            predicate = conjuncts.iterator().next();
        }
        else if (conjuncts.size() > 1) {
            predicate = new BooleanOperator(
                BooleanOperator.Type.AND_EXPR, conjuncts);
        }
        return predicate;
    }


    /**
     * This helper function takes a query plan and a selection predicate, and
     * adds the predicate to the plan in a reasonably intelligent way.
     * <p>
     * If the plan is a subclass of the {@link SelectNode} then the select
     * node's predicate is updated to include the predicate.  Specifically, if
     * the select node already has a predicate then one of the following occurs:
     * <ul>
     *   <li>If the select node currently has no predicate, the new predicate is
     *       assigned to the select node.</li>
     *   <li>If the select node has a predicate whose top node is a
     *       {@link BooleanOperator} of type <tt>AND</tt>, this predicate is
     *       added as a new term on that node.</li>
     *   <li>If the select node has some other kind of non-<tt>null</tt>
     *       predicate then this method creates a new top-level <tt>AND</tt>
     *       operation that will combine the two predicates into one.</li>
     * </ul>
     * <p>
     * If the plan is <em>not</em> a subclass of the {@link SelectNode} then a
     * new {@link SimpleFilterNode} is added above the current plan node, with
     * the specified predicate.
     *
     * @param plan the plan to add the selection predicate to
     *
     * @param predicate the selection predicate to add to the plan
     * 
     * @return the (possibly new) top plan-node for the plan with the selection
     *         predicate applied
     */
    private PlanNode addPredicateToPlan(PlanNode plan, Expression predicate) {
        if (plan instanceof SelectNode) {
            SelectNode selectNode = (SelectNode) plan;

            if (selectNode.predicate != null) {
                // There is already an existing predicate.  Add this as a
                // conjunct to the existing predicate.
                Expression fsPred = selectNode.predicate;
                boolean handled = false;

                // If the current predicate is an AND operation, just make
                // the where-expression an additional term.
                if (fsPred instanceof BooleanOperator) {
                    BooleanOperator bool = (BooleanOperator) fsPred;
                    if (bool.getType() == BooleanOperator.Type.AND_EXPR) {
                        bool.addTerm(predicate);
                        handled = true;
                    }
                }

                if (!handled) {
                    // Oops, the current file-scan predicate wasn't an AND.
                    // Create an AND expression instead.
                    BooleanOperator bool =
                        new BooleanOperator(BooleanOperator.Type.AND_EXPR);
                    bool.addTerm(fsPred);
                    bool.addTerm(predicate);
                    selectNode.predicate = bool;
                }
            }
            else {
                // Simple - just add where-expression onto the file-scan.
                selectNode.predicate = predicate;
            }
        }
        else {
            // The subplan is more complex, so put a filter node above it.
            plan = new SimpleFilterNode(plan, predicate);
        }

        return plan;
    }


    /**
     * This helper method takes a collection of expressions, and finds those
     * expressions that can be evaluated solely against the provided set of one
     * or more schemas.  In other words, if an expression doesn't refer to any
     * symbols outside of the specified set of schemas, then it will be included
     * in the result collection.
     *
     * @param srcExprs the input collection of expressions to check against the
     *        provided schemas.
     *
     * @param remove if <tt>true</tt>, the matching expressions will be removed
     *        from the <tt>srcExprs</tt> collection.  Otherwise, the
     *        <tt>srcExprs</tt> collection is left unchanged.
     *
     * @param dstExprs the collection to add the matching expressions to.  This
     *        collection is <tt>not</tt> cleared by this method; any previous
     *        contents in the collection will be left unchanged.
     *
     * @param schemas a collection of one or more schemas to check the input
     *        expressions against.  If an expression can be evaluated solely
     *        against these schemas then it will be added to the results.
     */
    public static void findExprsUsingSchemas(Collection<Expression> srcExprs,
        boolean remove, Collection<Expression> dstExprs, Schema... schemas) {

        ArrayList<ColumnName> symbols = new ArrayList<ColumnName>();

        Iterator<Expression> termIter = srcExprs.iterator();
        while (termIter.hasNext()) {
            Expression term = termIter.next();

            // Read all symbols from this term.
            symbols.clear();
            term.getAllSymbols(symbols);

            // If *all* of the symbols in the term reference at least one of the
            // provided schemas, add it to the results (removing from this
            // operator, if so directed by caller).
            boolean allRef = true;
            for (ColumnName colName : symbols) {
                // Determine if *this* symbol references at least one schema.
                boolean ref = false;
                for (Schema schema : schemas) {
                    if (schema.getColumnIndex(colName) != -1) {
                        ref = true;
                        break;
                    }
                }

                // If this symbol doesn't reference any of the schemas then
                // this term doesn't qualify.
                if (!ref) {
                    allRef = false;
                    break;
                }
            }

            if (allRef) {
                dstExprs.add(term);
                if (remove)
                    termIter.remove();
            }
        }
    }


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
     *     the clause is a join of two relations.  <em>This case should be
     *     handled by the {@link #makePlan} method using a dynamic programming
     *     technique, so this method throws an exception if it encounters a join
     *     expression.
     *   </li>
     * </ul>
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression, or has some other unrecognized type.
     */
    public PlanNode makeFromPlan(FromClause fromClause)
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
        return new FileScanNode(tableInfo, predicate);
    }
}
