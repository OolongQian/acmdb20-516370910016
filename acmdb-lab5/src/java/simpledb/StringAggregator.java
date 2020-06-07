package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
	
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private boolean useGroup;
	
	class Group {
		private LinkedList<String> strings = new LinkedList<>();
		private Op what;
		
		Group(Op what) {
			this.what = what;
		}
		
		private void mergeField(StringField field) {
			strings.add(field.getValue());
		}
		
		private Field getAggregateVal() {
			switch (what) {
				case COUNT:
					return new IntField(strings.size());
				default:
					throw new RuntimeException("what value error.");
			}
		}
	}
	
	/**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
	    this.gbfield = gbfield;
	    this.gbfieldtype = gbfieldtype;
	    this.afield = afield;
	    this.what = what;
	    useGroup = (gbfieldtype != null);
    }
	
	private Map<Field, Group> groups = new HashMap<>();
	private Group noGroup = new Group(what);
	
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

	    StringField attrStr = (StringField) tup.getField(afield);
	    
	    if (!useGroup) {
		    noGroup.mergeField(attrStr);
	    } else {
		    Field gbField = tup.getField(gbfield);
		    if (!groups.containsKey(gbField))
			    groups.put(gbField, new Group(what));
		    groups.get(gbField).mergeField(attrStr);
	    }
	    
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
			        types[0] = gbfieldtype;
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
