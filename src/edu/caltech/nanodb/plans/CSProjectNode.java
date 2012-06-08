package edu.caltech.nanodb.plans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.ColumnValue;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.PlanNode.OperationType;
import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.PlanCost;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.colstore.CSGeneratedTuple;

public class CSProjectNode extends PlanNode {
	
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CSProjectNode.class);
    
    /** The new schema that this project node creates */
    private List<SelectValue> projectionSpec;

    /** The table to select from if this node is a leaf. */
	private TableFileInfo tblFileInfo;
	
	/** The schema of tuples in the underlying table. */
    private Schema inputSchema;
    
    private ArrayList<CSFileScanNode> fileScanChildren;
    
    private Expression predicate;
    
    private CSSimpleFilterNode predNode;
    
    private boolean done;

    /**
     * This collection holds the non-wildcard column information, so that we can
     * more easily assign schema to projected tuples.
     */
    private List<ColumnInfo> nonWildcardColumnInfos;
	
	public CSProjectNode(SelectClause selClause, TableFileInfo tblFileInfo) {
		super(OperationType.PROJECT);
		
		predicate = selClause.getWhereExpr();
		projectionSpec = selClause.getSelectValues();
		this.tblFileInfo = tblFileInfo;
		inputSchema = this.tblFileInfo.getSchema();
		fileScanChildren = new ArrayList<CSFileScanNode>();
		done = false;
	}

	@Override
	public List<OrderByExpression> resultsOrderedBy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean supportsMarking() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean requiresLeftMarking() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean requiresRightMarking() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void prepare() {
		
		predNode = new CSSimpleFilterNode(tblFileInfo, predicate);
		
		schema = new Schema();
        nonWildcardColumnInfos = new ArrayList<ColumnInfo>();
        
        for (SelectValue selVal : projectionSpec) {
            if (selVal.isWildcard()) {
                schema.append(inputSchema);
                nonWildcardColumnInfos = schema.getColumnInfos();
                for (ColumnInfo colInfo : nonWildcardColumnInfos) {
                	fileScanChildren.add(new CSFileScanNode(tblFileInfo, colInfo, null));
                }
            }
            else if (selVal.isExpression()) {
                // Determining the schema is relatively straightforward.

                Expression expr = selVal.getExpression();
                ColumnInfo colInfo;

                if (expr instanceof ColumnValue) {
                    // This is a simple column-reference.  Pull out the schema
                    // and open the data file.
                    ColumnValue colValue = (ColumnValue) expr;
                    int colIndex = inputSchema.getColumnIndex(colValue.getColumnName());
                    colInfo = inputSchema.getColumnInfo(colIndex);
                    
                    fileScanChildren.add(new CSFileScanNode(tblFileInfo, colInfo, null));
                }
                else {
                    // This is a more complicated expression.  Guess the schema,
                    // and assume that every row will have a distinct value.

                    colInfo = expr.getColumnInfo(inputSchema);
                    
                    throw new UnsupportedOperationException(
                        "Complex project support is currently incomplete.");
                }

                // Apply any aliases here...
                String alias = selVal.getAlias();
                if (alias != null)
                    colInfo = new ColumnInfo(alias, colInfo.getType());

                schema.addColumnInfo(colInfo);
                nonWildcardColumnInfos.add(colInfo);
            }
            else if (selVal.isScalarSubquery()) {
                throw new UnsupportedOperationException(
                    "Scalar subquery support is currently incomplete.");
            }
        }
        
        predNode.prepare();
        
        for (CSFileScanNode node : fileScanChildren) {
        	try {
				node.prepare();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		
	}

	@Override
	public void markCurrentPosition() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void resetToLastMark() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanUp() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String toString() {
		String plan = "CSProject[values:  " + projectionSpec.toString() + "]\n";
		if (predicate != null) {
			plan = plan + "\t" + predNode.toString();
		}
		for (CSFileScanNode node : fileScanChildren) {
			plan = plan + "\t" + node.toString() + "\n";
		}
		return plan;
	}
	
	/** Do initialization for the select operation.  Resets state variables. */
    public void initialize() {
        super.initialize();
    }

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Tuple getNextTuple() throws IllegalStateException, IOException {
		if (done) return null;
		
		CSGeneratedTuple tuple = new CSGeneratedTuple(nonWildcardColumnInfos);

		Object temp;
		while (!predNode.getNext()) {
			for (int i = 0; i < fileScanChildren.size(); i++) {
				CSFileScanNode node = fileScanChildren.get(i);
				temp = node.getNextObject();
				if (temp == null) {
					done = true;
					return null;
				}
			}
		}
		
		for (int i = 0; i < fileScanChildren.size(); i++) {
			CSFileScanNode node = fileScanChildren.get(i);
			temp = node.getNextObject();
			if (temp == null) {
				done = true;
				return null;
			}
			tuple.setColumnValue(i, temp);
		}
		
		return tuple;
	}
	
	

}
