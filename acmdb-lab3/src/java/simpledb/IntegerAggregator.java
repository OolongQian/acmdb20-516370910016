package simpledb;

import com.sun.org.apache.bcel.internal.generic.LineNumberGen;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
	
	private static final long serialVersionUID = 1L;
	
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private boolean useGroup;
	
	class Group {
		private LinkedList<Integer> fields = new LinkedList<>();
		private Op what;
		
		Group(Op what) {
			this.what = what;
		}
		
		private void mergeField(IntField field) {
			fields.add(field.getValue());
		}
		
		private int compareFields(Integer f1, Integer f2) {
			if (f1 < f2)
				return -1;
			if (f1 == f2)
				return 0;
			return 1;
		}
		
		private Field getAggregateVal() {
			fields.sort(this::compareFields);
			int sum = 0;
			for (Integer field : fields)
				sum += field;
			int avg = sum / fields.size();
			
			switch (what) {
				case MIN:
					return new IntField(fields.getFirst());
				case MAX:
					return new IntField(fields.getLast());
				case SUM:
					return new IntField(sum);
				case AVG:
					return new IntField(avg);
				case COUNT:
					return new IntField(fields.size());
				case SC_AVG:
					throw new RuntimeException("have not implemented SC_AVG.");
				case SUM_COUNT:
					throw new RuntimeException("have not implemented SUM_COUNT.");
				default:
					throw new RuntimeException("what value error.");
			}
		}
	}
	
	private Map<Field, Group> groups = new HashMap<>();
	private Group noGroup;
	
	/**
	 * Aggregate constructor
	 *
	 * @param gbfield     the 0-based index of the group-by field in the tuple, or
	 *                    NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
	 *                    if there is no grouping
	 * @param afield      the 0-based index of the aggregate field in the tuple
	 * @param what        the aggregation operator
	 */
	
	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		// some code goes here
		this.gbfield = gbfield;
		this.gbfieldtype = gbfieldtype;
		this.afield = afield;
		this.what = what;
		this.useGroup = (gbfieldtype != null);
		this.noGroup = new Group(what);
	}
	
	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 *
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		// some code goes here
		
		IntField attrInt = (IntField) tup.getField(afield);
		
		if (!useGroup) {
			noGroup.mergeField(attrInt);
		} else {
			Field gbField = tup.getField(gbfield);
			if (!groups.containsKey(gbField))
				groups.put(gbField, new Group(what));
			
			groups.get(gbField).mergeField(attrInt);
		}
	}
	
	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 * if using group, or a single (aggregateVal) if no grouping. The
	 * aggregateVal is determined by the type of aggregate specified in
	 * the constructor.
	 */
	public DbIterator iterator() {
		// some code goes here
		return new DbIterator() {
			private boolean opened;
			Set<Map.Entry<Field, Group>> entrySet = groups.entrySet();
			private Iterator<Map.Entry<Field, Group>> groupIter;
			private boolean queried;
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				opened = true;
				if (useGroup)
					groupIter = entrySet.iterator();
				else
					queried = false;
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				if (!opened)
					throw new DbException("integer aggregator has not been opened. ");
				
				if (useGroup)
					return groupIter.hasNext();
				else
					return !queried;
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!hasNext())
					throw new NoSuchElementException("integer aggregator has been exhausted.");
				
				if (!useGroup) {
					queried = true;

					Tuple aggVal = new Tuple(getTupleDesc());
					
					aggVal.setField(0, noGroup.getAggregateVal());
					return aggVal;
					
				} else {
					Map.Entry<Field, Group> elem = groupIter.next();
					
					Tuple aggPair = new Tuple(getTupleDesc());
					
					aggPair.setField(0, elem.getKey());
					aggPair.setField(1, elem.getValue().getAggregateVal());
					
					return aggPair;
				}
			}
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				if (!opened)
					throw new DbException("integer aggregator has not been opened. ");
				
				if (!useGroup) {
					queried = false;
				} else {
					groupIter = entrySet.iterator();
				}
			}
			
			@Override
			public TupleDesc getTupleDesc() {
				if (!useGroup) {
					Type[] types = new Type[1];
					types[0] = Type.INT_TYPE;
					return new TupleDesc(types);
				} else {
					Type[] types = new Type[2];
					types[0] = gbfieldtype;
					types[1] = Type.INT_TYPE;
					return new TupleDesc(types);
				}
			}
			
			@Override
			public void close() {
				opened = false;
			}
		};
	}
}
