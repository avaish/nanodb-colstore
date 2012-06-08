package edu.caltech.nanodb.plans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.ColumnName;
import edu.caltech.nanodb.expressions.Environment;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.storage.TableFileInfo;
import edu.caltech.nanodb.storage.colstore.CSGeneratedTuple;

public class CSSimpleFilterNode {
	/** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CSSimpleFilterNode.class);
    
    /**
     * The environment used to evaluate expressions against tuples being
     * processed.
     */
    private Environment environment;
	
	private Expression predicate;
	
	private TableFileInfo tblFileInfo;
	
    private ArrayList<CSFileScanNode> fileScanChildren;
    
    private Schema schema;
    
    private ArrayList<ColumnInfo> infos;
    
    boolean done;

	public CSSimpleFilterNode(TableFileInfo tblFileInfo, Expression predicate) {
		this.predicate = predicate;
		this.tblFileInfo = tblFileInfo;
		fileScanChildren = new ArrayList<CSFileScanNode>();
		infos = new ArrayList<ColumnInfo>();
		schema = null;
		done = false;
		environment = new Environment();
	}

	public void prepare() {
		if (predicate == null) return;
		
		Schema prev = tblFileInfo.getSchema();
		schema = new Schema();
		ArrayList<ColumnName> symbols = new ArrayList<ColumnName>();
		predicate.getAllSymbols(symbols);
		
		ColumnInfo current;
		for (ColumnName name : symbols) {
			current = prev.getColumnInfo(prev.getColumnIndex(name));
			infos.add(current);
			fileScanChildren.add(new CSFileScanNode(tblFileInfo, current, null));
			schema.addColumnInfo(current);
		}
		
		for (CSFileScanNode node : fileScanChildren) {
        	try {
				node.prepare();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
		
		logger.debug(schema);	
	}
	
	public boolean getNext() throws IOException {
		if (done) return false;
		if (predicate == null) return true;
		
		CSGeneratedTuple tuple = new CSGeneratedTuple(infos);
		
		Object temp;
		for (int i = 0; i < fileScanChildren.size(); i++) {
			CSFileScanNode node = fileScanChildren.get(i);
			temp = node.getNextObject();
			if (temp == null) {
				done = true;
				return false;
			}
			tuple.setColumnValue(i, temp);
		}
		
		environment.clear();
        environment.addTuple(schema, tuple);
        return predicate.evaluatePredicate(environment);
	}
	
	public String toString() {
        String plan = "CSSimpleFilter[pred:  " + predicate.toString() + "]\n";
        for (CSFileScanNode node : fileScanChildren) {
			plan = plan + "\t\t" + node.toString() + "\n";
		}
        return plan;
    }

}
