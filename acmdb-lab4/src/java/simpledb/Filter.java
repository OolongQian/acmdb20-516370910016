package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate p;
    private DbIterator child;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
		return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
	    super.open();
	    child.open();
    }

    public void close() {
        // some code goes here
	    super.close();
	    child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	    super.close();
	    super.open();
	    child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
	    while (true) {
	    	
	    	if (!child.hasNext()) return null;
	    	
	    	Tuple tuple = child.next();
	    	if (p.filter(tuple)) return tuple;
	    	
	    }
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
