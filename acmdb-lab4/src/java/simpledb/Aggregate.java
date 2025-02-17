package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
	
	private static final long serialVersionUID = 1L;
	
	private DbIterator child;
	private TupleDesc childTd;
	private int afield;
	private int gfield;
	private Aggregator.Op aop;
	
	private Aggregator aggregator;
	private DbIterator aggIter;
	
	/**
	 * Constructor.
	 * <p>
	 * Implementation hint: depending on the type of afield, you will want to
	 * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
	 * you with your implementation of readNext().
	 *
	 * @param child  The DbIterator that is feeding us tuples.
	 * @param afield The column over which we are computing an aggregate.
	 * @param gfield The column over which we are grouping the result, or -1 if
	 *               there is no grouping
	 * @param aop    The aggregation operator to use
	 */
	public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		// some code goes here
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		childTd = child.getTupleDesc();
	}
	
	/**
	 * @return If this aggregate is accompanied by a groupby, return the groupby
	 * field index in the <b>INPUT</b> tuples. If not, return
	 * {@link simpledb.Aggregator#NO_GROUPING}
	 */
	public int groupField() {
		// some code goes here
		return gfield;
	}
	
	/**
	 * @return If this aggregate is accompanied by a group by, return the name
	 * of the groupby field in the <b>OUTPUT</b> tuples If not, return
	 * null;
	 */
	public String groupFieldName() {
		// some code goes here
		return childTd.getFieldName(gfield);
	}
	
	/**
	 * @return the aggregate field
	 */
	public int aggregateField() {
		// some code goes here
		return afield;
	}
	
	/**
	 * @return return the name of the aggregate field in the <b>OUTPUT</b>
	 * tuples
	 */
	public String aggregateFieldName() {
		// some code goes here
		return childTd.getFieldName(afield);
	}
	
	/**
	 * @return return the aggregate operator
	 */
	public Aggregator.Op aggregateOp() {
		// some code goes here
		return aop;
	}
	
	public static String nameOfAggregatorOp(Aggregator.Op aop) {
		return aop.toString();
	}
	
	public void open() throws NoSuchElementException, DbException,
			TransactionAbortedException {
		
		// match Operator super implementation.
		super.open();
		
		// create aggregator.
		// determine the attribute type.
		if (childTd.getFieldType(afield) == Type.INT_TYPE) {
			// if no grouping.
			if (gfield == -1) {
				aggregator = new IntegerAggregator(Aggregator.NO_GROUPING, null, afield, aop);
			} else {
				aggregator = new IntegerAggregator(gfield, childTd.getFieldType(gfield), afield, aop);
			}
		} else {
			assert childTd.getFieldType(afield) == Type.STRING_TYPE;
			if (gfield == -1) {
				aggregator = new StringAggregator(Aggregator.NO_GROUPING, null, afield, aop);
			} else {
				aggregator = new StringAggregator(gfield, childTd.getFieldType(gfield), afield, aop);
			}
		}
		
		// merge all tuples.
		child.open();
		while (child.hasNext()) {
			aggregator.mergeTupleIntoGroup(child.next());
		}
		aggIter = aggregator.iterator();
		aggIter.open();
	}
	
	/**
	 * Returns the next tuple. If there is a group by field, then the first
	 * field is the field by which we are grouping, and the second field is the
	 * result of computing the aggregate, If there is no group by field, then
	 * the result tuple should contain one field representing the result of the
	 * aggregate. Should return null if there are no more tuples.
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (aggIter.hasNext())
			return aggIter.next();
		else
			return null;
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		super.close();
		super.open();
		aggIter.rewind();
	}
	
	/**
	 * Returns the TupleDesc of this Aggregate. If there is no group by field,
	 * this will have one field - the aggregate column. If there is a group by
	 * field, the first field will be the group by field, and the second will be
	 * the aggregate value column.
	 * <p>
	 * The name of an aggregate column should be informative. For example:
	 * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
	 * given in the constructor, and child_td is the TupleDesc of the child
	 * iterator.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		TupleDesc anonymousAggTd = childTd;
		int numField = anonymousAggTd.numFields();
		
		Type[] types = new Type[numField];
		for (int i = 0; i < numField; ++i)
			types[i] = anonymousAggTd.getFieldType(i);
		
		String[] names = new String[numField];
		if (numField == 2)
			names[0] = childTd.getFieldName(gfield);
		names[numField - 1] = aop.name() + " " + childTd.getFieldName(afield);
		
		return new TupleDesc(types, names);
	}
	
	public void close() {
		// some code goes here
		super.close();
		aggIter.close();
		child.close();
	}
	
	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		DbIterator[] children = new DbIterator[1];
		children[0] = child;
		return children;
	}
	
	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		assert children.length == 1;
		child = children[0];
	}
	
}
