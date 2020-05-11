package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {
	
	private static final long serialVersionUID = 1L;
	
	private JoinPredicate p;
	private DbIterator child1, child2;
	
	/**
	 * Constructor. Accepts to children to join and the predicate to join them
	 * on
	 *
	 * @param p      The predicate to use to join the children
	 * @param child1 Iterator for the left(outer) relation to join
	 * @param child2 Iterator for the right(inner) relation to join
	 */
	public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
		// some code goes here
		assert p.getOperator() == Predicate.Op.EQUALS;
		this.p = p;
		this.child1 = child1;
		this.child2 = child2;
	}
	
	public JoinPredicate getJoinPredicate() {
		// some code goes here
		return p;
	}
	
	public TupleDesc getTupleDesc() {
		// some code goes here
		return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
	}
	
	public String getJoinField1Name() {
		// some code goes here
		TupleDesc tupleDesc1 = child1.getTupleDesc();
		int field1 = p.getField1();
		return tupleDesc1.getFieldName(field1);
	}
	
	public String getJoinField2Name() {
		// some code goes here
		TupleDesc tupleDesc2 = child2.getTupleDesc();
		int field2 = p.getField2();
		return tupleDesc2.getFieldName(field2);
	}
	
	public void open() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		// some code goes here
		super.open();
		listIt = performHashJoin();
	}
	
	public void close() {
		// some code goes here
		super.close();
		child1.close();
		child2.close();
		listIt = null;
	}
	
	public void rewind() throws DbException, TransactionAbortedException {
		// some code goes here
		super.close();
		super.open();
		((HashJoinIterator) listIt).rewind();
	}
	
	transient Iterator<Tuple> listIt = null;
	
	private Iterator<Tuple> performHashJoin() throws DbException, NoSuchElementException,
			TransactionAbortedException {
		return new HashJoinIterator();
	}
	
	/**
	 * Returns the next tuple generated by the join, or null if there are no
	 * more tuples. Logically, this is the next tuple in r1 cross r2 that
	 * satisfies the join predicate. There are many possible implementations;
	 * the simplest is a nested loops join.
	 * <p>
	 * Note that the tuples returned from this particular implementation of Join
	 * are simply the concatenation of joining tuples from the left and right
	 * relation. Therefore, there will be two copies of the join attribute in
	 * the results. (Removing such duplicate columns can be done with an
	 * additional projection operator if needed.)
	 * <p>
	 * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
	 * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
	 *
	 * @return The next matching tuple.
	 * @see JoinPredicate#filter
	 */
	protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		// some code goes here
		if (!listIt.hasNext()) return null;
		
		return listIt.next();
	}
	
	@Override
	public DbIterator[] getChildren() {
		// some code goes here
		DbIterator[] children = new DbIterator[2];
		children[0] = child1;
		children[1] = child2;
		return children;
	}
	
	@Override
	public void setChildren(DbIterator[] children) {
		// some code goes here
		assert children.length == 2;
		child1 = children[0];
		child2 = children[1];
	}
	
	private Tuple tupleMerge(Tuple t1, Tuple t2) {
		Tuple tJoin = new Tuple(getTupleDesc());
		int i = 0;
		
		for (Iterator<Field> iter = t1.fields(); iter.hasNext(); ++i)
			tJoin.setField(i, iter.next());
		
		for (Iterator<Field> iter = t2.fields(); iter.hasNext(); ++i)
			tJoin.setField(i, iter.next());
		
		return tJoin;
	}
	
	class HashJoinIterator implements Iterator<Tuple> {
		
		private LinkedList<LinkedList<HashAndTuple>> extendHashList = new LinkedList<>();
		private LinkedList<Tuple> joinedList = new LinkedList<>();
		private Iterator<Tuple> curIter;
		
		
		class HashAndTuple {
			Tuple tuple;
			Field field;
			int hash;
			int childIndex;
			
			HashAndTuple(Tuple t, Field f, int idx) {
				tuple = t;
				field = f;
				childIndex = idx;
				hash = f.hashCode();
			}
		}
		
		private int hashCompare(HashAndTuple ht1, HashAndTuple ht2) {
			if (ht1.hash < ht2.hash)
				return -1;
			if (ht1.hash == ht2.hash)
				return 0;
			return 1;
		}

//		public HashJoinIterator() throws DbException, NoSuchElementException,
//				TransactionAbortedException {
//			ArrayList<HashAndTuple> sortList = new ArrayList<>();
//
//			child1.open();
//			while (child1.hasNext()) {
//				Tuple t = child1.next();
//				Field f = t.getField(p.getField1());
//				sortList.add(new HashAndTuple(t, f, 1));
//			}
//
//			child2.open();
//			while (child2.hasNext()) {
//				Tuple t = child2.next();
//				Field f = t.getField(p.getField2());
//				sortList.add(new HashAndTuple(t, f, 2));
//			}
//
//			sortList.sort(this::hashCompare);
//
//
//			 fill extended hash list.
//			Iterator<HashAndTuple> iter = sortList.iterator();
//			if (!iter.hasNext()) return;
//
//			HashAndTuple elem = iter.next();
//			int curHash = elem.hash;
//
//			extendHashList.add(new LinkedList<>());
//			extendHashList.getLast().add(elem);
//
//			while (iter.hasNext()) {
//				elem = iter.next();
//				if (elem.hash == curHash)
//					extendHashList.getLast().add(elem);
//				else {
//					curHash = elem.hash;
//					extendHashList.add(new LinkedList<>());
//					extendHashList.getLast().add(elem);
//				}
//			}
//
//
//
//			 use extended hash list to create joined tuples list.
//			for (Iterator<LinkedList<HashAndTuple>> oIter = extendHashList.iterator(); oIter.hasNext(); ) {
//				LinkedList<HashAndTuple> list = oIter.next();
//				LinkedList<Tuple> tuples1 = new LinkedList<>();
//				LinkedList<Tuple> tuples2 = new LinkedList<>();
//
//				for (Iterator<HashAndTuple> iIter = list.iterator(); iIter.hasNext(); ) {
//					HashAndTuple ht = iIter.next();
//					if (ht.childIndex == 1)
//						tuples1.add(ht.tuple);
//					else {
//						assert ht.childIndex == 2;
//						tuples2.add(ht.tuple);
//					}
//				}
//
//				for (Iterator<Tuple> iter1 = tuples1.iterator(); iter1.hasNext(); ) {
//					Tuple tuple1 = iter1.next();
//					for (Iterator<Tuple> iter2 = tuples2.iterator(); iter2.hasNext(); ) {
//						Tuple tuple2 = iter2.next();
//						joinedList.add(tupleMerge(tuple1, tuple2));
//					}
//				}
//			}
		
		// reset current index.
//			curIter = joinedList.iterator();
//		}
		
		private HashJoinIterator() throws DbException, NoSuchElementException,
				TransactionAbortedException {
			Map<Integer, LinkedList<HashAndTuple>> mapList = new HashMap<>();
			
			child1.open();
			while (child1.hasNext()) {
				Tuple t = child1.next();
				Field f = t.getField(p.getField1());
				
				if (!mapList.containsKey(f.hashCode()))
					mapList.put(f.hashCode(), new LinkedList<>());
				mapList.get(f.hashCode()).add(new HashAndTuple(t, f, 1));
			}
			
			child2.open();
			while (child2.hasNext()) {
				Tuple t = child2.next();
				Field f = t.getField(p.getField1());
				
				if (!mapList.containsKey(f.hashCode()))
					mapList.put(f.hashCode(), new LinkedList<>());
				mapList.get(f.hashCode()).add(new HashAndTuple(t, f, 2));
			}
			
			for (Iterator<Map.Entry<Integer, LinkedList<HashAndTuple>>> oIter = mapList.entrySet().iterator(); oIter.hasNext(); ) {
				LinkedList<HashAndTuple> list = oIter.next().getValue();
				LinkedList<Tuple> tuples1 = new LinkedList<>();
				LinkedList<Tuple> tuples2 = new LinkedList<>();
				
				for (Iterator<HashAndTuple> iIter = list.iterator(); iIter.hasNext(); ) {
					HashAndTuple ht = iIter.next();
					if (ht.childIndex == 1)
						tuples1.add(ht.tuple);
					else {
						assert ht.childIndex == 2;
						tuples2.add(ht.tuple);
					}
				}
				
				for (Iterator<Tuple> iter1 = tuples1.iterator(); iter1.hasNext(); ) {
					Tuple tuple1 = iter1.next();
					for (Iterator<Tuple> iter2 = tuples2.iterator(); iter2.hasNext(); ) {
						Tuple tuple2 = iter2.next();
						joinedList.add(tupleMerge(tuple1, tuple2));
					}
				}
			}
			
			// reset current index.
			curIter = joinedList.iterator();
		}
		
		@Override
		public boolean hasNext() {
			return curIter.hasNext();
		}
		
		@Override
		public Tuple next() {
			return curIter.next();
		}
		
		public void rewind() {
			curIter = joinedList.iterator();
		}
	}
}
